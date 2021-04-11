/*
 * @CopyRight:
 * FISCO-BCOS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * FISCO-BCOS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with FISCO-BCOS.  If not, see <http://www.gnu.org/licenses/>
 * (c) 2016-2018 fisco-dev contributors.
 */
/**
 * @brief : Sync implementation
 * @author: jimmyshi
 * @date: 2018-10-15
 */

#include "SyncMaster.h"
#include <json/json.h>
#include <libblockchain/BlockChainInterface.h>

using namespace std;
using namespace dev;
using namespace dev::eth;
using namespace dev::sync;
using namespace dev::p2p;
using namespace dev::blockchain;
using namespace dev::txpool;
using namespace dev::blockverifier;

void SyncMaster::printSyncInfo()
{
    auto pendingSize = m_txPool->pendingSize();//交易池中交易数量
    auto peers = m_syncStatus->peers();
    std::string peer_str;
    for (auto const& peer : *peers)
    {
        peer_str += peer.abridged() + "/";
    }
    SYNC_LOG(TRACE) << "\n[Sync Info] --------------------------------------------\n"
                    << "            IsSyncing:    " << isSyncing() << "\n"
                    << "            Block number: " << m_blockChain->number() << "\n"
                    << "            Block hash:   "
                    << m_blockChain->numberHash(m_blockChain->number()) << "\n"
                    << "            Genesis hash: " << m_syncStatus->genesisHash.abridged() << "\n"
                    << "            TxPool size:  " << pendingSize << "\n"
                    << "            Peers size:   " << peers->size() << "\n"
                    << "[Peer Info] --------------------------------------------\n"
                    << "    Host: " << m_nodeId.abridged() << "\n"
                    << "    Peer: " << peer_str << "\n"
                    << "            --------------------------------------------";
}

SyncStatus SyncMaster::status() const
{
    SyncStatus res;
    res.state = m_syncStatus->state;
    res.protocolId = m_protocolId;
    res.currentBlockNumber = m_blockChain->number();
    res.knownHighestNumber = m_syncStatus->knownHighestNumber;
    res.knownLatestHash = m_syncStatus->knownLatestHash;
    return res;
}

string const SyncMaster::syncInfo() const
{
    Json::Value syncInfo;
    syncInfo["isSyncing"] = isSyncing();
    syncInfo["protocolId"] = m_protocolId;
    syncInfo["genesisHash"] = toHex(m_syncStatus->genesisHash);
    syncInfo["nodeId"] = toHex(m_nodeId);

    int64_t currentNumber = m_blockChain->number();
    syncInfo["blockNumber"] = currentNumber;
    syncInfo["latestHash"] = toHex(m_blockChain->numberHash(currentNumber));
    syncInfo["knownHighestNumber"] = m_syncStatus->knownHighestNumber;
    syncInfo["knownLatestHash"] = toHex(m_syncStatus->knownLatestHash);
    syncInfo["txPoolSize"] = std::to_string(m_txPool->pendingSize());

    Json::Value peersInfo(Json::arrayValue);
    m_syncStatus->foreachPeer([&](shared_ptr<SyncPeerStatus> _p) {
        Json::Value info;
        info["nodeId"] = toHex(_p->nodeId);
        info["genesisHash"] = toHex(_p->genesisHash);
        info["blockNumber"] = _p->number;
        info["latestHash"] = toHex(_p->latestHash);
        peersInfo.append(info);
        return true;
    });

    syncInfo["peers"] = peersInfo;
    Json::FastWriter fastWriter;
    std::string statusStr = fastWriter.write(syncInfo);
    return statusStr;
}

void SyncMaster::start()
{
    startWorking();
    m_syncTrans->start();
    if (m_blockStatusGossipThread)
    {
        m_blockStatusGossipThread->start();
    }
}

void SyncMaster::stop()
{
    // stop SynMsgEngine
    if (m_msgEngine)
    {
        m_msgEngine->stop();
    }
    if (m_downloadBlockProcessor)
    {
        m_downloadBlockProcessor->stop();
    }
    if (m_sendBlockProcessor)
    {
        m_sendBlockProcessor->stop();
    }
    m_syncTrans->stop();
    if (m_blockStatusGossipThread)
    {
        m_blockStatusGossipThread->stop();
    }
    doneWorking();
    stopWorking();
    // will not restart worker, so terminate it
    terminate();
    SYNC_LOG(INFO) << LOG_BADGE("g:" + std::to_string(m_groupId)) << LOG_DESC("SyncMaster stopped");
}

void SyncMaster::doWork()
{
    // Debug print
    if (isSyncing())    //同步状态：空闲/下载中
        printSyncInfo();
    // maintain the connections between observers/sealers
    maintainPeersConnection();//维护（链接）同步节点列表
    m_downloadBlockProcessor->enqueue([this]() {
        try
        {
            // flush downloaded buffer into downloading queue
            maintainDownloadingQueueBuffer();//downloading:把区块缓存 转入 队列存放    idle:清空
            // Not Idle do
            if (isSyncing())
            {
                // check and commit the downloaded block
                if (m_syncStatus->state == SyncState::Downloading)
                {
                    bool finished = maintainDownloadingQueue();//commit blocks
                    if (finished)
                        noteDownloadingFinish();
                }
            }
            // send block-download-request to peers if this node is behind others
            maintainPeersStatus();//若符合条件，则变成downloading，发送 下载请求
        }
        catch (std::exception const& e)
        {
            SYNC_LOG(ERROR) << LOG_DESC(
                                   "maintainDownloadingQueue or maintainPeersStatus exceptioned")
                            << LOG_KV("errorInfo", boost::diagnostic_information(e));
        }
    });
    // send block-status to other nodes when commit a new block
    maintainBlocks();//发送 同步状态消息包       新区块上链发/5s发一次
    // send block to other nodes
    m_sendBlockProcessor->enqueue([this]() {
        try
        {
            maintainBlockRequest();//回复下载请求，发送区块。每次只给200ms时间发送
        }
        catch (std::exception const& e)
        {
            SYNC_LOG(ERROR) << LOG_DESC("maintainBlockRequest exceptioned")
                            << LOG_KV("errorInfo", boost::diagnostic_information(e));
        }
    });
}

void SyncMaster::workLoop()
{
    while (workerState() == WorkerState::Started)
    {
        doWork();
        if (idleWaitMs())
        {
            boost::unique_lock<boost::mutex> l(x_signalled);
            m_signalled.wait_for(l, boost::chrono::milliseconds(idleWaitMs()));
        }
    }
}


void SyncMaster::noteSealingBlockNumber(int64_t _number)
{
    WriteGuard l(x_currentSealingNumber);
    m_currentSealingNumber = _number;
    m_signalled.notify_all();
}

bool SyncMaster::isSyncing() const
{
    return m_syncStatus->state != SyncState::Idle;
}

// is my number is far smaller than max block number of this block chain
bool SyncMaster::blockNumberFarBehind() const
{
    return m_msgEngine->blockNumberFarBehind();
}

void SyncMaster::maintainBlocks()
{
    if (!m_needSendStatus)
    {
        return;
    }

    if (!m_newBlocks && utcSteadyTime() <= m_maintainBlocksTimeout)//往下走的条件 1 新区块上链  2 无新区块上链但超时5s
    {
        return;
    }

    m_newBlocks = false;
    m_maintainBlocksTimeout = utcSteadyTime() + c_maintainBlocksTimeout;


    int64_t number = m_blockChain->number();
    h256 const& currentHash = m_blockChain->numberHash(number); //？？？   引用

    if (m_syncTreeRouter)
    {
        // sendSyncStatus by tree
        sendSyncStatusByTree(number, currentHash);
    }
    else
    {
        // broadcast status
        broadcastSyncStatus(number, currentHash);
    }
}


void SyncMaster::sendSyncStatusByTree(BlockNumber const& _blockNumber, h256 const& _currentHash)
{
    auto selectedNodes = m_syncTreeRouter->selectNodesForBlockSync(m_syncStatus->peersSet());
    SYNC_LOG(DEBUG) << LOG_BADGE("sendSyncStatusByTree") << LOG_KV("blockNumber", _blockNumber)
                    << LOG_KV("currentHash", _currentHash.abridged())
                    << LOG_KV("selectedNodes", selectedNodes->size());
    for (auto const& nodeId : *selectedNodes)
    {
        sendSyncStatusByNodeId(_blockNumber, _currentHash, nodeId);
    }
}

void SyncMaster::broadcastSyncStatus(BlockNumber const& _blockNumber, h256 const& _currentHash)
{
    m_syncStatus->foreachPeer([&](shared_ptr<SyncPeerStatus> _p) {
        return sendSyncStatusByNodeId(_blockNumber, _currentHash, _p->nodeId);
    });
}

bool SyncMaster::sendSyncStatusByNodeId(
    BlockNumber const& blockNumber, h256 const& currentHash, dev::network::NodeID const& nodeId)
{
    auto packet = m_syncMsgPacketFactory->createSyncStatusPacket(
        m_nodeId, blockNumber, m_genesisHash, currentHash);
    packet->alignedTime = utcTime();
    packet->encode();
    m_service->asyncSendMessageByNodeID(
        nodeId, packet->toMessage(m_protocolId), CallbackFuncWithSession(), Options());
    SYNC_LOG(DEBUG) << LOG_BADGE("Status") << LOG_DESC("Send current status when maintainBlocks")
                    << LOG_KV("number", blockNumber)
                    << LOG_KV("genesisHash", m_genesisHash.abridged())
                    << LOG_KV("currentHash", currentHash.abridged())
                    << LOG_KV("peer", nodeId.abridged())
                    << LOG_KV("currentTime", packet->alignedTime);
    return true;
}

void SyncMaster::maintainPeersStatus()
{
    uint64_t currentTime = utcTime();
    if (isSyncing())//threr is a diffience with 2.0.0 :每发送一个区块下载请求后 需要等一会儿在发送同步请求？ //每个区块200ms时间
    {
        auto maxTimeout = std::max((int64_t)c_respondDownloadRequestTimeout,
            (int64_t)m_eachBlockDownloadingRequestTimeout *
                (m_maxRequestNumber - m_lastDownloadingBlockNumber));
        // Skip downloading if last if not timeout
        if (((int64_t)currentTime - (int64_t)m_lastDownloadingRequestTime) < maxTimeout)
        {
            return;  // no need to sync
        }
        else
        {
            SYNC_LOG(DEBUG) << LOG_BADGE("Download") << LOG_DESC("timeout and request a new block")
                            << LOG_KV("maxRequestNumber", m_maxRequestNumber)
                            << LOG_KV("lastDownloadingBlockNumber", m_lastDownloadingBlockNumber);
        }
    }

    // need download? ->set syncing and knownHighestNumber
    int64_t currentNumber = m_blockChain->number();
    int64_t maxPeerNumber = 0;
    h256 latestHash;
    m_syncStatus->foreachPeer([&](shared_ptr<SyncPeerStatus> _p) {//找到我知道的 最高区块
        if (_p->number > maxPeerNumber)
        {
            latestHash = _p->latestHash;
            maxPeerNumber = _p->number;
        }
        return true;
    });

    // update my known  //更新 变量 （已知的 最高区块）
    if (maxPeerNumber > currentNumber)
    {
        WriteGuard l(m_syncStatus->x_known);
        m_syncStatus->knownHighestNumber = maxPeerNumber;
        m_syncStatus->knownLatestHash = latestHash;
    }
    else
    {
        // No need to send sync request
        WriteGuard l(m_syncStatus->x_known);
        m_syncStatus->knownHighestNumber = currentNumber;
        m_syncStatus->knownLatestHash = m_blockChain->numberHash(currentNumber);
        return;
    }

    // Not to start download when mining or no need     peer中的最高区块号<= 本节点正在打包的区块号  或者 和本节点最高区块号相同时，不需要去同步
    {
        ReadGuard l(x_currentSealingNumber);
        if (maxPeerNumber <= m_currentSealingNumber || maxPeerNumber == currentNumber)
        {
            // mining : maxPeerNumber - currentNumber == 1
            // no need: maxPeerNumber - currentNumber <= 0
            SYNC_LOG(TRACE) << LOG_BADGE("Download") << LOG_DESC("No need to download")
                            << LOG_KV("currentNumber", currentNumber)
                            << LOG_KV("currentSealingNumber", m_currentSealingNumber)
                            << LOG_KV("maxPeerNumber", maxPeerNumber);
            return;  // no need to sync
        }
    }

    m_lastDownloadingRequestTime = currentTime;
    m_lastDownloadingBlockNumber = currentNumber;

    // Start download
    noteDownloadingBegin();

    // Choose to use min number in blockqueue or max peer number    1. 本节点区块号 >= peers中已知的最高区块号 时候，不需要去同步
    int64_t maxRequestNumber = maxPeerNumber;
    BlockPtr topBlock = m_syncStatus->bq().top();
    if (nullptr != topBlock)
    {
        int64_t minNumberInQueue = topBlock->header().number();
        maxRequestNumber = min(maxPeerNumber, minNumberInQueue - 1);//2.本节点区块号 >= 下载队列中最小区块号时，说明正在同步中，不需要去同步
    }
    if (currentNumber >= maxRequestNumber)
    {
        SYNC_LOG(TRACE) << LOG_BADGE("Download")
                        << LOG_DESC("No need to sync when blocks are already in queue")
                        << LOG_KV("currentNumber", currentNumber)
                        << LOG_KV("maxRequestNumber", maxRequestNumber);
        return;  // no need to send request block packet
    }

    // adjust maxRequestBlocksSize before request blocks
    m_syncStatus->bq().adjustMaxRequestBlocks();
    // Sharding by c_maxRequestBlocks to request blocks   向每个peer请求最大区块数是32，求出需要发送请求消息的peer的个数
    auto requestBlocksSize = m_syncStatus->bq().maxRequestBlocks();
    if (requestBlocksSize <= 0)
    {
        return;
    }
    size_t shardNumber =//请求32个区块内，对一个节点请求，多余32个区块，像2个节点请求
        (maxRequestNumber - currentNumber + requestBlocksSize - 1) / requestBlocksSize;
    size_t shard = 0;//表示从第几个节点取区块

    m_maxRequestNumber = 0;  // each request turn has new m_maxRequestNumber
    while (shard < shardNumber && shard < c_maxRequestShards)
    {
        bool thisTurnFound = false; //标记用的，当前节点是不是 有最高区块 的节点
        m_syncStatus->foreachPeerRandom([&](std::shared_ptr<SyncPeerStatus> _p) {
            if (m_syncStatus->knownHighestNumber <= 0 ||
                _p->number != m_syncStatus->knownHighestNumber)
            {
                // Only send request to nodes which are not syncing(has max number)
                return true;
            }

            // shard: [from, to]
            int64_t from = currentNumber + 1 + shard * requestBlocksSize;
            int64_t to = min(from + requestBlocksSize - 1, maxRequestNumber);
            if (_p->number < to)
                return true;  // exit, to next peer    没啥用

            // found a peer
            thisTurnFound = true;
            SyncReqBlockPacket packet;
            unsigned size = to - from + 1;
            packet.encode(from, size);
            m_service->asyncSendMessageByNodeID(//给节点发请求区块消息
                _p->nodeId, packet.toMessage(m_protocolId), CallbackFuncWithSession(), Options());

            // update max request number   这条很奇怪，没啥用
            m_maxRequestNumber = max(m_maxRequestNumber, to);

            SYNC_LOG(INFO) << LOG_BADGE("Download") << LOG_BADGE("Request")
                           << LOG_DESC("Request blocks") << LOG_KV("frm", from) << LOG_KV("to", to)
                           << LOG_KV("peer", _p->nodeId.abridged());

            ++shard;  // shard move

            return shard < shardNumber && shard < c_maxRequestShards;
        });

        if (!thisTurnFound)//链接的节点数量 小于 收集的同步状态数量，并且链接的节点高度都小于已知的最高区块
        {
            int64_t from = currentNumber + shard * requestBlocksSize;
            int64_t to = min(from + requestBlocksSize - 1, maxRequestNumber);

            SYNC_LOG(WARNING) << LOG_BADGE("Download") << LOG_BADGE("Request")
                              << LOG_DESC("Couldn't find any peers to request blocks")
                              << LOG_KV("from", from) << LOG_KV("to", to);
            break;
        }
    }
}

bool SyncMaster::maintainDownloadingQueue()
{
    int64_t currentNumber = m_blockChain->number();
    DownloadingBlockQueue& bq = m_syncStatus->bq();
    if (currentNumber >= m_syncStatus->knownHighestNumber)//current block number > known highest blknumber, clear downloading queue
    {
        bq.clear();
        return true;
    }

    // pop block in sequence and ignore block which number is lower than currentNumber +1
    BlockPtr topBlock = bq.top();
    if (topBlock && topBlock->header().number() > (m_blockChain->number() + 1))//check queue' top block  until currentNumber+1 and clear
    {
        SYNC_LOG(DEBUG) << LOG_DESC("Discontinuous block")
                        << LOG_KV("topNumber", topBlock->header().number())
                        << LOG_KV("curNumber", m_blockChain->number());
    }
    while (topBlock != nullptr && topBlock->header().number() <= (m_blockChain->number() + 1))
    {
        try
        {
            if (isWorking() && isNextBlock(topBlock))
            {
                auto record_time = utcTime();
                auto parentBlock =
                    m_blockChain->getBlockByNumber(topBlock->blockHeader().number() - 1);
                BlockInfo parentBlockInfo{parentBlock->header().hash(),
                    parentBlock->header().number(), parentBlock->header().stateRoot()};
                auto getBlockByNumber_time_cost = utcTime() - record_time;
                record_time = utcTime();
                SYNC_LOG(INFO) << LOG_BADGE("Download") << LOG_BADGE("BlockSync")
                               << LOG_DESC("Download block execute")
                               << LOG_KV("number", topBlock->header().number())
                               << LOG_KV("txs", topBlock->transactions()->size())
                               << LOG_KV("hash", topBlock->headerHash().abridged());
                ExecutiveContext::Ptr exeCtx =
                    m_blockVerifier->executeBlock(*topBlock, parentBlockInfo);

                if (exeCtx == nullptr)
                {
                    bq.pop();
                    topBlock = bq.top();//  maybe it have several blocks of  same block number  in priority queue
                    continue;
                }

                auto executeBlock_time_cost = utcTime() - record_time;
                record_time = utcTime();

                CommitResult ret = m_blockChain->commitBlock(topBlock, exeCtx);
                auto commitBlock_time_cost = utcTime() - record_time;
                record_time = utcTime();
                if (ret == CommitResult::OK)
                {
                    m_txPool->dropBlockTrans(topBlock);
                    auto dropBlockTrans_time_cost = utcTime() - record_time;
                    SYNC_LOG(INFO) << LOG_BADGE("Download") << LOG_BADGE("BlockSync")
                                   << LOG_DESC("Download block commit succ")
                                   << LOG_KV("number", topBlock->header().number())
                                   << LOG_KV("txs", topBlock->transactions()->size())
                                   << LOG_KV("hash", topBlock->headerHash().abridged())
                                   << LOG_KV("getBlockByNumberTimeCost", getBlockByNumber_time_cost)
                                   << LOG_KV("executeBlockTimeCost", executeBlock_time_cost)
                                   << LOG_KV("commitBlockTimeCost", commitBlock_time_cost)
                                   << LOG_KV("dropBlockTransTimeCost", dropBlockTrans_time_cost);
                }
                else
                {
                    SYNC_LOG(ERROR) << LOG_BADGE("Download") << LOG_BADGE("BlockSync")
                                    << LOG_DESC("Block commit failed")
                                    << LOG_KV("number", topBlock->header().number())
                                    << LOG_KV("txs", topBlock->transactions()->size())
                                    << LOG_KV("hash", topBlock->headerHash().abridged());
                }
            }
            else
            {
                SYNC_LOG(DEBUG) << LOG_BADGE("Download") << LOG_BADGE("BlockSync")
                                << LOG_DESC("Block of queue top is not the next block")
                                << LOG_KV("number", topBlock->header().number())
                                << LOG_KV("txs", topBlock->transactions()->size())
                                << LOG_KV("hash", topBlock->headerHash().abridged());
            }
        }
        catch (exception& e)
        {
            SYNC_LOG(ERROR) << LOG_BADGE("Download") << LOG_BADGE("BlockSync")
                            << LOG_DESC("Block of queue top is not a valid block")
                            << LOG_KV("number", topBlock->header().number())
                            << LOG_KV("txs", topBlock->transactions()->size())
                            << LOG_KV("hash", topBlock->headerHash().abridged());
        }

        bq.pop();
        topBlock = bq.top();
    }


    currentNumber = m_blockChain->number();
    // has this request turn finished ?
    if (currentNumber >= m_maxRequestNumber)// after sended maxRequest has reveived,  notify  immidiately to continue to send request
        m_lastDownloadingRequestTime = 0;  // reset it to trigger request immediately

    // has download finished ?
    if (currentNumber >= m_syncStatus->knownHighestNumber)
    {
        h256 const& latestHash =
            m_blockChain->getBlockByNumber(m_syncStatus->knownHighestNumber)->headerHash();
        SYNC_LOG(INFO) << LOG_BADGE("Download") << LOG_BADGE("BlockSync")
                       << LOG_DESC("Download finish") << LOG_KV("latestHash", latestHash.abridged())
                       << LOG_KV("expectedHash", m_syncStatus->knownLatestHash.abridged());

        if (m_syncStatus->knownLatestHash != latestHash)
            SYNC_LOG(ERROR)
                << LOG_BADGE("Download")
                << LOG_DESC(
                       "State error: This node's version is not compatable with others! All data "
                       "should be cleared of this node before restart");

        return true;
    }
    return false;
}

void SyncMaster::maintainPeersConnection()
{
    // Get active peers                                                                                                         从p2p获取链接的有效节点
    auto sessions = m_service->sessionInfosByProtocolID(m_protocolId);
    set<NodeID> activePeers;
    for (auto const& session : sessions)
    {
        activePeers.insert(session.nodeID());
    }

    // Get sealers and observer
    NodeIDs sealers = m_blockChain->sealerList();
    NodeIDs sealerOrObserver = sealers + m_blockChain->observerList();

    // member set is [(sealer || observer) && activePeer && not myself]    /获取可链接的节点  memberSet表示 sealer+observer中与当前节点有链接的节点集，不包含自己
    set<NodeID> memberSet;
    bool hasMyself = false;//本节点时候在共识列表中
    for (auto const& member : sealerOrObserver)
    {
        /// find active peers
        if (activePeers.find(member) != activePeers.end() && member != m_nodeId)
        {
            memberSet.insert(member);
        }
        hasMyself |= (member == m_nodeId);
    }

    // Delete uncorrelated peers                                                                                   删除同步节点列表中，与当前节点 没有链接 的节点（没有链接但是区块高度大于自己则不删除）
    int64_t currentNumber = m_blockChain->number();
    NodeIDs peersToDelete;
    m_syncStatus->foreachPeer([&](std::shared_ptr<SyncPeerStatus> _p) {
        NodeID id = _p->nodeId;
        if (memberSet.find(id) == memberSet.end() && currentNumber >= _p->number)
        {
            // Only delete outsider whose number is smaller than myself
            peersToDelete.emplace_back(id);
        }
        return true;
    });

    for (NodeID const& id : peersToDelete)
    {
        m_syncStatus->deletePeer(id);
    }


    // Add new peers                                                                                                               在同步列表中添加，
    h256 const& currentHash = m_blockChain->numberHash(currentNumber);//根据链接  增加新的活跃节点到 同步节点集中
    for (auto const& member : memberSet)
    {
        if (member != m_nodeId && !m_syncStatus->hasPeer(member))
        {
            // create a peer
            auto newPeerStatus = m_syncMsgPacketFactory->createSyncStatusPacket(
                member, 0, m_genesisHash, m_genesisHash);
            m_syncStatus->newSyncPeerStatus(newPeerStatus);

            if (m_needSendStatus)
            {
                // send my status to her        //发送本节点状态 给 新加入的节点
                auto packet = m_syncMsgPacketFactory->createSyncStatusPacket(
                    m_nodeId, currentNumber, m_genesisHash, currentHash);
                packet->alignedTime = utcTime();
                packet->encode();

                m_service->asyncSendMessageByNodeID(
                    member, packet->toMessage(m_protocolId), CallbackFuncWithSession(), Options());
                SYNC_LOG(DEBUG) << LOG_BADGE("Status")
                                << LOG_DESC("Send current status to new peer")
                                << LOG_KV("number", int(currentNumber))
                                << LOG_KV("genesisHash", m_genesisHash.abridged())
                                << LOG_KV("currentHash", currentHash.abridged())
                                << LOG_KV("peer", member.abridged())
                                << LOG_KV("currentTime", packet->alignedTime);
            }
        }
    }

    // Update sync sealer status    //更新 同步节点集中的节点 sealer 标识
    set<NodeID> sealerSet;
    for (auto sealer : sealers)
        sealerSet.insert(sealer);

    m_syncStatus->foreachPeer([&](shared_ptr<SyncPeerStatus> _p) {
        _p->isSealer = (sealerSet.find(_p->nodeId) != sealerSet.end());
        return true;
    });

    // If myself is not in group, no need to maintain transactions(send transactions to peers)
    m_syncTrans->updateNeedMaintainTransactions(hasMyself);

    // If myself is not in group, no need to maintain blocks(send sync status to peers)
    m_needSendStatus = m_isGroupMember || hasMyself;  // need to send if last time is in group
    m_txQueue->setNeedImportToTxPool(m_needSendStatus);
    m_isGroupMember = hasMyself;
}

void SyncMaster::maintainDownloadingQueueBuffer()
{
    if (m_syncStatus->state == SyncState::Downloading) //进入下载流程  则把缓存导入 下载队列
    {
        m_syncStatus->bq().clearFullQueueIfNotHas(m_blockChain->number() + 1);
        m_syncStatus->bq().flushBufferToQueue();//把下载的区块缓存 倒入 下载队列，并清空缓存
    }
    else
        m_syncStatus->bq().clear();//若处于idle，清空下载队列 和 下载缓存
}

void SyncMaster::maintainBlockRequest()
{
    uint64_t timeout = utcSteadyTime() + c_respondDownloadRequestTimeout; //200ms
    m_syncStatus->foreachPeerRandom([&](std::shared_ptr<SyncPeerStatus> _p) {
        DownloadRequestQueue& reqQueue = _p->reqQueue;//维护的每个 syncPeer 的下载请求 队列，这个队列是怎么维护的？
        if (reqQueue.empty())
            return true;  // no need to respond   队列为空，则不需要respond

        // Just select one peer per maintain
        DownloadBlocksContainer blockContainer(m_service, m_protocolId, _p->nodeId);

        while (!reqQueue.empty() && utcSteadyTime() <= timeout)//若没超时   200ms    ？？？sync   将peer的请求队列from size取出
        {
            DownloadRequest req = reqQueue.topAndPop();//每看懂？？？sync
            int64_t number = req.fromNumber;
            int64_t numberLimit = req.fromNumber + req.size;
            SYNC_LOG(DEBUG) << LOG_BADGE("Download Request: response blocks")
                            << LOG_KV("from", req.fromNumber) << LOG_KV("size", req.size)
                            << LOG_KV("numberLimit", numberLimit)
                            << LOG_KV("peer", _p->nodeId.abridged());
            // Send block at sequence
            for (; number < numberLimit && utcSteadyTime() <= timeout; number++)// 将【from size】区块放入区块容器，满1M则发送||超时200ms发送
            {
                auto start_get_block_time = utcTime();
                shared_ptr<bytes> blockRLP = m_blockChain->getBlockRLPByNumber(number);
                if (!blockRLP)
                {
                    SYNC_LOG(WARNING)
                        << LOG_BADGE("Download") << LOG_BADGE("Request")
                        << LOG_DESC("Get block for node failed")
                        << LOG_KV("reason", "block is null") << LOG_KV("number", number)
                        << LOG_KV("nodeId", _p->nodeId.abridged());
                    break;
                }
                auto requiredPermits = blockRLP->size() / g_BCOSConfig.c_compressRate;
                if (m_nodeBandwidthLimiter && !m_nodeBandwidthLimiter->tryAcquire(requiredPermits))
                {
                    SYNC_LOG(INFO)
                        << LOG_BADGE("maintainBlockRequest")
                        << LOG_DESC("stop responding block for over the channel bandwidth limit")
                        << LOG_KV("peer", _p->nodeId.abridged());
                    break;
                }
                // over the network-bandwidth-limiter
                if (m_bandwidthLimiter && !m_bandwidthLimiter->tryAcquire(requiredPermits))
                {
                    SYNC_LOG(INFO) << LOG_BADGE("maintainBlockRequest")
                                   << LOG_DESC("stop responding block for over the bandwidth limit")
                                   << LOG_KV("peer", _p->nodeId.abridged());
                    break;
                }
                SYNC_LOG(INFO) << LOG_BADGE("Download") << LOG_BADGE("Request")
                               << LOG_BADGE("BlockSync") << LOG_DESC("Batch blocks for sending")
                               << LOG_KV("blockSize", blockRLP->size()) << LOG_KV("number", number)
                               << LOG_KV("peer", _p->nodeId.abridged())
                               << LOG_KV("timeCost", utcTime() - start_get_block_time);
                blockContainer.batchAndSend(blockRLP); //积累区块达到 > 1M-2048 or 超过200ms ， 发送给peer
            }

            if (number < numberLimit)  // This respond not reach the end due to timeout
            {
                // write back the rest request range
                SYNC_LOG(DEBUG) << LOG_BADGE("Download") << LOG_BADGE("Request")
                                << LOG_DESC("Push unsent requests back to reqQueue")
                                << LOG_KV("from", number) << LOG_KV("to", numberLimit - 1)
                                << LOG_KV("peer", _p->nodeId.abridged());
                reqQueue.push(number, numberLimit - number);
                return false;
            }
        }
        return false;
    });
}

bool SyncMaster::isNextBlock(BlockPtr _block)
{
    if (_block == nullptr)
        return false;

    int64_t currentNumber = m_blockChain->number();
    if (currentNumber + 1 != _block->header().number())
    {
        SYNC_LOG(WARNING) << LOG_BADGE("Download") << LOG_BADGE("BlockSync")
                          << LOG_DESC("Ignore illegal block") << LOG_KV("reason", "number illegal")
                          << LOG_KV("thisNumber", _block->header().number())
                          << LOG_KV("currentNumber", currentNumber);
        return false;
    }

    if (m_blockChain->numberHash(currentNumber) != _block->header().parentHash())
    {
        SYNC_LOG(WARNING) << LOG_BADGE("Download") << LOG_BADGE("BlockSync")
                          << LOG_DESC("Ignore illegal block")
                          << LOG_KV("reason", "parent hash illegal")
                          << LOG_KV("thisNumber", _block->header().number())
                          << LOG_KV("currentNumber", currentNumber)
                          << LOG_KV("thisParentHash", _block->header().parentHash().abridged())
                          << LOG_KV(
                                 "currentHash", m_blockChain->numberHash(currentNumber).abridged());
        return false;
    }

    // check block sealerlist sig
    if (fp_isConsensusOk && !(fp_isConsensusOk)(*_block))
    {
        SYNC_LOG(WARNING) << LOG_BADGE("Download") << LOG_BADGE("BlockSync")
                          << LOG_DESC("Ignore illegal block")
                          << LOG_KV("reason", "consensus check failed")
                          << LOG_KV("thisNumber", _block->header().number())
                          << LOG_KV("currentNumber", currentNumber)
                          << LOG_KV("thisParentHash", _block->header().parentHash().abridged())
                          << LOG_KV(
                                 "currentHash", m_blockChain->numberHash(currentNumber).abridged());
        return false;
    }

    return true;
}

void SyncMaster::sendBlockStatus(int64_t const& _gossipPeersNumber)
{
    auto blockNumber = m_blockChain->number();
    auto currentHash = m_blockChain->numberHash(blockNumber);
    m_syncStatus->forRandomPeers(_gossipPeersNumber, [&](std::shared_ptr<SyncPeerStatus> _p) {
        if (_p)
        {
            return sendSyncStatusByNodeId(blockNumber, currentHash, _p->nodeId);
        }
        return true;
    });
}