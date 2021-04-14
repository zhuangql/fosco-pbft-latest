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
    maintainPeersConnection();//维护当前node的同步表
    m_downloadBlockProcessor->enqueue([this]() {
        try
        {
            // flush downloaded buffer into downloading queue
            maintainDownloadingQueueBuffer();//在downloading状态下，buffer->blocks queue.
            // Not Idle do
            if (isSyncing())
            {
                // check and commit the downloaded block
                if (m_syncStatus->state == SyncState::Downloading)
                {
                    bool finished = maintainDownloadingQueue();//queue中的 blocks上链
                    if (finished)
                        noteDownloadingFinish();
                }
            }
            // send block-download-request to peers if this node is behind others
            maintainPeersStatus();//若节点落后，则发送  下载区块req
        }
        catch (std::exception const& e)
        {
            SYNC_LOG(ERROR) << LOG_DESC(
                                   "maintainDownloadingQueue or maintainPeersStatus exceptioned")
                            << LOG_KV("errorInfo", boost::diagnostic_information(e));
        }
    });
    // send block-status to other nodes when commit a new block
    maintainBlocks();//发送sync status包，新区块commit发 || 间隔5s发一次
    // send block to other nodes
    m_sendBlockProcessor->enqueue([this]() {
        try
        {
            maintainBlockRequest();//回复blocks   0.64s超时    为什么新启一个线程还要有timeout
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
//1、群组内的节点才互发sync status
//2、新区块commit发一次 || 间隔5s发一次
void SyncMaster::maintainBlocks()
{
    if (!m_needSendStatus)//群组内node才发sync status
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
//发区块req
//1、控制发区块req的速度
//2、找到已知的最高块，判断是否需要发req
//3、需要同步，进入start downloading状态
//4、选择要同步的最高区块：区块queue中的最小区块（queue断连情况），还是同步表中的最大区块
//5、计算要向几个node发req，计算req的区块区间，发送
void SyncMaster::maintainPeersStatus()
{
    uint64_t currentTime = utcTime();
    if (isSyncing())//1、idle下，可直接发req；2、downleading下，控制发req的速度；
    {//？？？不清晰
        auto maxTimeout = std::max((int64_t)c_respondDownloadRequestTimeout,//0.64s，回复下载区块req的timeout；超时则重新请求 ？？？什么场景干嘛的
            (int64_t)m_eachBlockDownloadingRequestTimeout *    //每个区块的下载请求timeout 是 1s
                (m_maxRequestNumber - m_lastDownloadingBlockNumber));//上一次startDownload时所请求的区块数量*1s
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
    int64_t currentNumber = m_blockChain->number();//当前链最高height
    int64_t maxPeerNumber = 0;//同步表中的最高height
    h256 latestHash;//同步表中最高区块的hash
    m_syncStatus->foreachPeer([&](shared_ptr<SyncPeerStatus> _p) {//找到我知道的 最高区块
        if (_p->number > maxPeerNumber)
        {
            latestHash = _p->latestHash;
            maxPeerNumber = _p->number;
        }
        return true;
    });

    // update my known  //若节点落后，更新masterStarus （已知最高区块）
    if (maxPeerNumber > currentNumber)
    {
        WriteGuard l(m_syncStatus->x_known);
        m_syncStatus->knownHighestNumber = maxPeerNumber;
        m_syncStatus->knownLatestHash = latestHash;
    }
    else
    {
        // No need to send sync request   节点不落后，不需要发送同步区块req
        WriteGuard l(m_syncStatus->x_known);
        m_syncStatus->knownHighestNumber = currentNumber;
        m_syncStatus->knownLatestHash = m_blockChain->numberHash(currentNumber);
        return;
    }

    // Not to start download when mining or no need     没有必要同步区块；
    {   //1、节点落后一个块，但节点正在打包共识阶段，不发req     2、多此一举
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

    // Choose to use min number in blockqueue or max peer number优化     选择要同步的最高区块：区块queue中的最小区块（queue断连情况），还是同步表中的最大区块
    int64_t maxRequestNumber = maxPeerNumber;//maxRequestNumber表示本轮要发送的最大区块req号；有两种可能 1、同步表中的highest 2、下载区块queue中断连，则是queue.top()-1；
    BlockPtr topBlock = m_syncStatus->bq().top();//下载queue中的最小高度 区块
    if (nullptr != topBlock)
    {
        int64_t minNumberInQueue = topBlock->header().number();//下载队列中的最小区块号
        maxRequestNumber = min(maxPeerNumber, minNumberInQueue - 1);//min(同步表中的最高height 不多此一举,   queue.top() - 1）   queue断连情况？
    }
    if (currentNumber >= maxRequestNumber)//1. 节点currentHeight >= 同步表中最大高度，节点不落后，不需要同步（多此一举）
    {                                                                                       //2.节点currentHeight >= queue.top() - 1，节点落后但区块已经在queue中，不需要同步
        SYNC_LOG(TRACE) << LOG_BADGE("Download")
                        << LOG_DESC("No need to sync when blocks are already in queue")
                        << LOG_KV("currentNumber", currentNumber)
                        << LOG_KV("maxRequestNumber", maxRequestNumber);
        return;  // no need to send request block packet
    }

    // adjust maxRequestBlocksSize before request blocks   优化
    m_syncStatus->bq().adjustMaxRequestBlocks();
    // Sharding by c_maxRequestBlocks to request blocks   向每个peer请求最大区块数是32
    auto requestBlocksSize = m_syncStatus->bq().maxRequestBlocks();
    if (requestBlocksSize <= 0)
    {
        return;
    }
    size_t shardNumber =//需要对几个peer发区块req;请求<32个区块，向一个peer请求；>32个，向2个节点请求
        (maxRequestNumber - currentNumber + requestBlocksSize - 1) / requestBlocksSize;
    size_t shard = 0;//对peer计数，最多4个

    m_maxRequestNumber = 0;  ////上一次start downloading 发出的最大请求区块号 each request turn has new m_maxRequestNumber
    while (shard < shardNumber && shard < c_maxRequestShards)//不多于4个req
    {
        bool thisTurnFound = false; //标记用的，当前节点是不是 有最高区块 的节点，向其请求区块
        m_syncStatus->foreachPeerRandom([&](std::shared_ptr<SyncPeerStatus> _p) {//同步表中找node发req
            if (m_syncStatus->knownHighestNumber <= 0 ||//刚加入网络的node 忽略
                _p->number != m_syncStatus->knownHighestNumber)//查找有最高区块的节点
            {
                // Only send request to nodes which are not syncing(has max number)
                return true;
            }

            // shard: [from, to]   计算req区间
            int64_t from = currentNumber + 1 + shard * requestBlocksSize;
            int64_t to = min(from + requestBlocksSize - 1, maxRequestNumber);
            if (_p->number < to)
                return true;  // exit, to next peer    容错，没用

            // found a peer
            thisTurnFound = true;
            SyncReqBlockPacket packet;
            unsigned size = to - from + 1;
            packet.encode(from, size);
            m_service->asyncSendMessageByNodeID(//给此节点发区块req
                _p->nodeId, packet.toMessage(m_protocolId), CallbackFuncWithSession(), Options());

            // update max request number
            m_maxRequestNumber = max(m_maxRequestNumber, to);

            SYNC_LOG(INFO) << LOG_BADGE("Download") << LOG_BADGE("Request")
                           << LOG_DESC("Request blocks") << LOG_KV("frm", from) << LOG_KV("to", to)
                           << LOG_KV("peer", _p->nodeId.abridged());

            ++shard;  // shard move

            return shard < shardNumber && shard < c_maxRequestShards;
        });

        if (!thisTurnFound)//同步表中的nodes 全部没有最高区块，不发req
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
//queqe中区块上链
bool SyncMaster::maintainDownloadingQueue()
{
    int64_t currentNumber = m_blockChain->number();
    DownloadingBlockQueue& bq = m_syncStatus->bq();
    if (currentNumber >= m_syncStatus->knownHighestNumber)//当前区块高度>syncStatus最高区块，不用同步
    {
        bq.clear();
        return true;
    }

    // pop block in sequence and ignore block which number is lower than currentNumber +1
    BlockPtr topBlock = bq.top();
    if (topBlock && topBlock->header().number() > (m_blockChain->number() + 1))
    {
        SYNC_LOG(DEBUG) << LOG_DESC("Discontinuous block")
                        << LOG_KV("topNumber", topBlock->header().number())
                        << LOG_KV("curNumber", m_blockChain->number());
    }
    while (topBlock != nullptr && topBlock->header().number() <= (m_blockChain->number() + 1))//queue.top()等于currentHeight+1时，才上链
    {   //若queue.top()能接上链，将queqe区块全部处理，即使中间有断连（后果：从断连开始之后的区块需要重新请求和接收）
        try
        {
            if (isWorking() && isNextBlock(topBlock))//是否是下一区块（高度/区块父hash/区块体签名）
            {
                auto record_time = utcTime();
                auto parentBlock =
                    m_blockChain->getBlockByNumber(topBlock->blockHeader().number() - 1);
                BlockInfo parentBlockInfo{parentBlock->header().hash(),
                    parentBlock->header().number(), parentBlock->header().stateRoot()};//状态根hash 需要看一下
                auto getBlockByNumber_time_cost = utcTime() - record_time;
                record_time = utcTime();
                SYNC_LOG(INFO) << LOG_BADGE("Download") << LOG_BADGE("BlockSync")
                               << LOG_DESC("Download block execute")
                               << LOG_KV("number", topBlock->header().number())
                               << LOG_KV("txs", topBlock->transactions()->size())
                               << LOG_KV("hash", topBlock->headerHash().abridged());
                ExecutiveContext::Ptr exeCtx =
                    m_blockVerifier->executeBlock(*topBlock, parentBlockInfo);

                if (exeCtx == nullptr)//执行不成功
                {
                    bq.pop();
                    topBlock = bq.top();// 在queue中可能存在多个同一高度的blocks
                    continue;
                }

                auto executeBlock_time_cost = utcTime() - record_time;
                record_time = utcTime();

                CommitResult ret = m_blockChain->commitBlock(topBlock, exeCtx);//
                auto commitBlock_time_cost = utcTime() - record_time;
                record_time = utcTime();
                if (ret == CommitResult::OK)
                {
                    m_txPool->dropBlockTrans(topBlock);//处理交易池中的上链交易？
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
    if (currentNumber >= m_maxRequestNumber)//优化；若发送的最大区块request已经上链，立刻继续发送区块request；场景：请求的区块过多不能一次请求完
        m_lastDownloadingRequestTime = 0;  // reset it to trigger request immediately

    // has download finished ?    本node的区块链是否和active node 保持一致
    if (currentNumber >= m_syncStatus->knownHighestNumber)
    {
        h256 const& latestHash =
            m_blockChain->getBlockByNumber(m_syncStatus->knownHighestNumber)->headerHash();
        SYNC_LOG(INFO) << LOG_BADGE("Download") << LOG_BADGE("BlockSync")
                       << LOG_DESC("Download finish") << LOG_KV("latestHash", latestHash.abridged())
                       << LOG_KV("expectedHash", m_syncStatus->knownLatestHash.abridged());

        if (m_syncStatus->knownLatestHash != latestHash)//版本容错
            SYNC_LOG(ERROR)
                << LOG_BADGE("Download")
                << LOG_DESC(
                       "State error: This node's version is not compatable with others! All data "
                       "should be cleared of this node before restart");

        return true;//下载完成
    }
    return false;//下载未完成，要继续下载
}

//获取可同步的节点（sealer && obserber）
//1、删除同步列表中的失效节点
//2、在同步列表中增加 新加入的节点(同时给新节点发送sync status包)
//3、更新同步表（的节点角色，sealer or observer，因此频率应该不低于出块速度）
void SyncMaster::maintainPeersConnection()
{
    // Get active peers                                                                                                         从p2p获取链接的有效节点activePeers
    auto sessions = m_service->sessionInfosByProtocolID(m_protocolId);
    set<NodeID> activePeers;
    for (auto const& session : sessions)
    {
        activePeers.insert(session.nodeID());
    }

    // Get sealers and observer
    NodeIDs sealers = m_blockChain->sealerList();
    NodeIDs sealerOrObserver = sealers + m_blockChain->observerList();

    // member set is [(sealer || observer) && activePeer && not myself] 获取可同步的节点  memberSet表示 actiePeers中的sealer+observer节点，不包含自己
    set<NodeID> memberSet;
    bool hasMyself = false;//本节点是否在群组中
    for (auto const& member : sealerOrObserver)
    {
        /// find active peers
        if (activePeers.find(member) != activePeers.end() && member != m_nodeId)
        {
            memberSet.insert(member);
        }
        hasMyself |= (member == m_nodeId);
    }

    // Delete uncorrelated peers                 删除同步节点列表中的断连节点，与当前node断连 的节点（断连但是区块高度>自己的不删除）
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
    h256 const& currentHash = m_blockChain->numberHash(currentNumber);//根据有效链接  增加新的活跃节点到 同步节点集中
    for (auto const& member : memberSet)
    {
        if (member != m_nodeId && !m_syncStatus->hasPeer(member))
        {
            // create a peer
            auto newPeerStatus = m_syncMsgPacketFactory->createSyncStatusPacket(
                member, 0, m_genesisHash, m_genesisHash);
            m_syncStatus->newSyncPeerStatus(newPeerStatus);

            if (m_needSendStatus)//
            {
                // send my status to her        //发送本节点状态 给 新加入的节点
                auto packet = m_syncMsgPacketFactory->createSyncStatusPacket(
                    m_nodeId, currentNumber, m_genesisHash, currentHash);
                packet->alignedTime = utcTime();
                packet->encode();

                m_service->asyncSendMessageByNodeID(//异步发送？？？
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

    // Update sync sealer status    //更新 同步节点集中的节点 sealer 标识    sealer角色在同步中的作用是什么？
    set<NodeID> sealerSet;
    for (auto sealer : sealers)
        sealerSet.insert(sealer);

    m_syncStatus->foreachPeer([&](shared_ptr<SyncPeerStatus> _p) {
        _p->isSealer = (sealerSet.find(_p->nodeId) != sealerSet.end());
        return true;
    });

    // If myself is not in group, no need to maintain transactions(send transactions to peers)
    m_syncTrans->updateNeedMaintainTransactions(hasMyself);

    // If myself is not in group, no need to maintain blocks(send sync status to peers)  群组外的节点，不用发syncStatus给其他节点
    m_needSendStatus = m_isGroupMember || hasMyself;
    m_txQueue->setNeedImportToTxPool(m_needSendStatus);
    m_isGroupMember = hasMyself;
}

void SyncMaster::maintainDownloadingQueueBuffer()//节点初始start时state为idle，什么也不做
{
    if (m_syncStatus->state == SyncState::Downloading) //进入下载流程  则把缓存导入 下载队列
    {
        m_syncStatus->bq().clearFullQueueIfNotHas(m_blockChain->number() + 1);//容错
        m_syncStatus->bq().flushBufferToQueue();//将p2p区块list  剪切至-> 下载queue
    }
    else
        m_syncStatus->bq().clear();//若处于idle，不需要下载，clear下载队列
}
//1、随机选择（请求区块的）节点 回复 blocks  ，只回复0.64s
//2、发送时  不一次性全发，1M 1M的发
//3、超时却没发完，把没发完的block 区间 放回 peer请求队列
void SyncMaster::maintainBlockRequest()
{
    uint64_t timeout = utcSteadyTime() + c_respondDownloadRequestTimeout; //0.64s       200ms*32
    m_syncStatus->foreachPeerRandom([&](std::shared_ptr<SyncPeerStatus> _p) {//随机选节点respond Blocks，最多respond 0.64s
        DownloadRequestQueue& reqQueue = _p->reqQueue;//节点的block req 队列
        if (reqQueue.empty())
            return true;  // no need to respond   队列为空，则不需要respond

        // Just select one peer per maintain
        DownloadBlocksContainer blockContainer(m_service, m_protocolId, _p->nodeId);//回复blocks的engine  为什么单独定义一个？

        while (!reqQueue.empty() && utcSteadyTime() <= timeout)//0.64s内的操作
        {
            DownloadRequest req = reqQueue.topAndPop();//怎么合并的？？？   取出[from, size]
            int64_t number = req.fromNumber;
            int64_t numberLimit = req.fromNumber + req.size;
            SYNC_LOG(DEBUG) << LOG_BADGE("Download Request: response blocks")
                            << LOG_KV("from", req.fromNumber) << LOG_KV("size", req.size)
                            << LOG_KV("numberLimit", numberLimit)
                            << LOG_KV("peer", _p->nodeId.abridged());
            // Send block at sequence
            for (; number < numberLimit && utcSteadyTime() <= timeout; number++)// 将[from, size]区块放入 blockContainer，满1M则发送||超过0.64s发送
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
                auto requiredPermits = blockRLP->size() / g_BCOSConfig.c_compressRate;//没看；after 2.0.0
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
                blockContainer.batchAndSend(blockRLP); //积累区块达到 > 1M-2048 or 超过0.64s ， 发送给peer
            }

            if (number < numberLimit)  //由于超时  没发完[number，numberLimit]，把没发完的写回peer 的请求队列
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
    if (currentNumber + 1 != _block->header().number())//比较高度是否能接上
    {
        SYNC_LOG(WARNING) << LOG_BADGE("Download") << LOG_BADGE("BlockSync")
                          << LOG_DESC("Ignore illegal block") << LOG_KV("reason", "number illegal")
                          << LOG_KV("thisNumber", _block->header().number())
                          << LOG_KV("currentNumber", currentNumber);
        return false;
    }

    if (m_blockChain->numberHash(currentNumber) != _block->header().parentHash())//比较hash能否接上
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

    // check block sealerlist sig   检查区块签名
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