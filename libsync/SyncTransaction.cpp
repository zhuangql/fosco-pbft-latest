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
 * (c) 2016-2019 fisco-dev contributors.
 */
/**
 * @brief : implementation of sync transaction
 * @author: yujiechen
 * @date: 2019-09-16
 */

#include "SyncTransaction.h"
#include "SyncMsgPacket.h"
#include <json/json.h>

using namespace std;
using namespace dev;
using namespace dev::eth;
using namespace dev::sync;
using namespace dev::p2p;
using namespace dev::txpool;

static unsigned const c_maxSendTransactions = 1000;

void SyncTransaction::start()
{
    startWorking();
    m_running.store(true);
    SYNC_LOG(DEBUG) << LOG_DESC("start SyncTransaction") << LOG_KV("groupId", m_groupId);
}

void SyncTransaction::stop()
{
    if (m_running.load())
    {
        m_running.store(false);
        doneWorking();
        stopWorking();
        // will not restart worker, so terminate it
        terminate();
        SYNC_LOG(DEBUG) << LOG_DESC("stop SyncTransaction") << LOG_KV("groupId", m_groupId);
    }
    else
    {
        SYNC_LOG(DEBUG) << LOG_DESC("SyncTransaction already stopped")
                        << LOG_KV("groupId", m_groupId);
    }
}

void SyncTransaction::doWork()
{
    maintainDownloadingTransactions();//tx下载queue->txPool

    // only maintain transactions for the nodes inner the group
    //1、node在群组内才maintain tx  2、交易池有新交易插入，唤醒交易同步线程；无，不广播；  3、优先处理完下载队列的tx，在广播tx
    if (m_needMaintainTransactions && m_newTransactions && m_txQueue->bufferSize() == 0)
    {
        maintainTransactions();//广播tx
    }

    if (m_needForwardRemainTxs)//rpbft用的
    {
        forwardRemainingTxs();
    }
}

void SyncTransaction::workLoop()
{
    while (workerState() == WorkerState::Started)
    {
        doWork();
        // no new transactions and the size of transactions need to be broadcasted is zero
        if (idleWaitMs() && !m_newTransactions && m_txQueue->bufferSize() == 0)
        {
            boost::unique_lock<boost::mutex> l(x_signalled);
            m_signalled.wait_for(l, boost::chrono::milliseconds(idleWaitMs()));
        }
    }
}
//1、从txPool取出没sync过的tx，每次最多取1000笔
//2、只广播给   同步表peers&&共识节点
void SyncTransaction::maintainTransactions()
{
    auto ts = m_txPool->topTransactionsCondition(c_maxSendTransactions, m_nodeId);//从txPool取1000笔未sync的tx（rpc tx）
    auto txSize = ts->size();
    if (txSize == 0)//txPool没有需要sync的tx，不广播
    {
        m_newTransactions = false;//txPool的前1000笔交易全同步过，变为false？
        return;
    }
    sendTransactions(ts, false, 0);//广播all  &&  广播 25% txHash
}
//发给谁：当前轮共识  同步表peers && sealer list
void SyncTransaction::sendTransactions(std::shared_ptr<Transactions> _ts,
    bool const& _fastForwardRemainTxs, int64_t const& _startIndex)
{
    std::shared_ptr<NodeIDs> selectedPeers;//要发的peers：当前轮共识  同步表peers && sealer list
    std::shared_ptr<std::set<dev::h512>> peers = m_syncStatus->peersSet();//取出同步表中的peers
    // fastforward remaining transactions
    if (_fastForwardRemainTxs)//false rpbft
    {
        // copy m_fastForwardedNodes to selectedPeers in case of m_fastForwardedNodes changed
        selectedPeers = std::make_shared<NodeIDs>();
        *selectedPeers = *m_fastForwardedNodes;
    }
    else
    {
        // only broadcastTransactions to the consensus nodes
        if (fp_txsReceiversFilter)//在pbft construct function中注册的回调
        {
            selectedPeers = fp_txsReceiversFilter(peers);//tx只广播给 当前轮共识  同步表peers && sealer list
        }
        else
        {
            selectedPeers = m_syncStatus->peers();//什么情况下发生？
        }
    }

    // send the transactions from RPC                           广播来自rpc的tx
    broadcastTransactions(selectedPeers, _ts, _fastForwardRemainTxs, _startIndex);//（发给谁，发哪些tx，0，0）
    if (!_fastForwardRemainTxs && m_running.load())
    {
        // Added sleep to prevent excessive redundant transaction message packets caused by
        // transaction status spreading too fast
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        //其他节点收到p2p发送的tx，转发一次25%的status逻辑在哪？？？
        sendTxsStatus(_ts, selectedPeers);//随机选25%节点  广播 tx  hash（自己发一次all，在发送25%？？？）
    }
}
//1、从selectedPeers中再次过滤需要发送tx的节点（过滤规则：本节点没发过此tx/只给sealer发tx/对方peer没有此tx）
//2、统计出 给closen的节点 发符合条件的txs
//3、给closen nodes 发 txs
void SyncTransaction::broadcastTransactions(std::shared_ptr<NodeIDs> _selectedPeers,
    std::shared_ptr<Transactions> _ts, bool const& _fastForwardRemainTxs,
    int64_t const& _startIndex)//（发给谁，发哪些tx，0，0）
{
    unordered_map<NodeID, std::vector<size_t>> peerTransactions;//给节点ID  发某些vec tx
    auto endIndex =
        std::min((int64_t)(_startIndex + c_maxSendTransactions - 1), (int64_t)(_ts->size() - 1));//tx的数量

    auto randomSelectedPeers = _selectedPeers;//发给谁   tree？
    bool randomSelectedPeersInited = false;//发给谁  是否已随机？
    int64_t consIndex = 0;//no
    if (m_treeRouter)//false
    {
        consIndex = m_treeRouter->consIndex();
    }
    for (ssize_t i = _startIndex; i <= endIndex; ++i)                                                                                            //处理每个tx
    {
        auto t = (*_ts)[i];//单个tx
        NodeIDs peers;//chosen的node

        int64_t selectSize = _selectedPeers->size();//发给几个节点
        // add redundancy when receive transactions from P2P                                                                  来自p2p的tx过滤掉不发
        if ((!t->rpcTx() || t->isKnownBySomeone()) && !_fastForwardRemainTxs)
        {
            continue;
        }
        if (m_treeRouter && !randomSelectedPeersInited && !_fastForwardRemainTxs)//不走
        {
            randomSelectedPeers =
                m_treeRouter->selectNodes(m_syncStatus->peersSet(), consIndex, true);
            randomSelectedPeersInited = true;
        }
        // the randomSelectedPeers is empty  不发给任何peers  应该是treeT用的
        if (randomSelectedPeers->size() == 0)
        {
            randomSelectedPeersInited = false;
            continue;
        }
        peers = m_syncStatus->filterPeers(
            selectSize, randomSelectedPeers, [&](std::shared_ptr<SyncPeerStatus> _p) {
                bool unsent = !t->isTheNodeContainsTransaction(m_nodeId) || _fastForwardRemainTxs;//本节点没有发送过此tx（发过的不再发）
                bool isSealer = _p->isSealer;//要发给sealer
                return isSealer && unsent && !t->isTheNodeContainsTransaction(_p->nodeId);//要发给的peer没有此tx（对同一node只发一次）
            });

        t->appendNodeContainsTransaction(m_nodeId);//告诉peers 本节点已有此tx
        if (0 == peers.size())//不需要发此tx，处理下一个tx
            continue;
        for (auto const& p : peers)
        {
            peerTransactions[p].push_back(i);//给某个节点p 发那些tx  i
            t->appendNodeContainsTransaction(p);//给p发tx之前，标记p已经有tx（表示本节点已经p发过了，其他节点不用给p发了）
        }
    }

    m_syncStatus->foreachPeerRandom([&](shared_ptr<SyncPeerStatus> _p) {//给所有chosen节点 发 txs
        std::vector<bytes> txRLPs;
        unsigned txsSize = peerTransactions[_p->nodeId].size();//给nodeid发tx的数量（key不存在则创建一个pair并调用默认构造函数）
        if (0 == txsSize)//tx为0不发
            return true;  // No need to send

        for (auto const& i : peerTransactions[_p->nodeId])//需要给nodeid 发的 txs
        {
            txRLPs.emplace_back((*_ts)[i]->rlp(WithSignature));//rlp序列化所有tx
        }


        std::shared_ptr<SyncTransactionsPacket> packet = std::make_shared<SyncTransactionsPacket>();
        if (m_treeRouter)//false
        {
            packet->encode(txRLPs, true, consIndex);
        }
        else
        {
            packet->encode(txRLPs);//编码所有tx
        }
        auto msg = packet->toMessage(m_protocolId, (!_fastForwardRemainTxs));//来自rpc
        m_service->asyncSendMessageByNodeID(_p->nodeId, msg, CallbackFuncWithSession(), Options());
        SYNC_LOG(DEBUG) << LOG_BADGE("Tx") << LOG_DESC("Send transaction to peer")
                        << LOG_KV("txNum", int(txsSize))
                        << LOG_KV("fastForwardRemainTxs", _fastForwardRemainTxs)
                        << LOG_KV("startIndex", _startIndex)
                        << LOG_KV("toNodeId", _p->nodeId.abridged())
                        << LOG_KV("messageSize(B)", msg->buffer()->size());
        return true;
    });
}

void SyncTransaction::forwardRemainingTxs()
{
    Guard l(m_fastForwardMutex);
    int64_t currentTxsSize = m_txPool->pendingSize();
    // no need to forward remaining transactions if the txpool is empty
    if (currentTxsSize == 0)
    {
        return;
    }
    auto ts = m_txPool->topTransactions(currentTxsSize);
    int64_t startIndex = 0;
    while (startIndex < currentTxsSize)
    {
        sendTransactions(ts, m_needForwardRemainTxs, startIndex);
        startIndex += c_maxSendTransactions;
    }
    for (auto const& targetNode : *m_fastForwardedNodes)
    {
        SYNC_LOG(DEBUG) << LOG_DESC("forwardRemainingTxs") << LOG_KV("txsSize", currentTxsSize)
                        << LOG_KV("targetNode", targetNode.abridged());
    }
    m_needForwardRemainTxs = false;
    m_fastForwardedNodes->clear();
}

void SyncTransaction::maintainDownloadingTransactions()
{
    m_txQueue->pop2TxPool(m_txPool);
}

// send transaction hash
void SyncTransaction::sendTxsStatus(
    std::shared_ptr<dev::eth::Transactions> _txs, std::shared_ptr<NodeIDs> _selectedPeers)
{
    unsigned percent = 25;
    unsigned expectedSelectSize = (_selectedPeers->size() * percent + 99) / 100;//selecetedPeers数量的25%
    int64_t selectSize = std::min(expectedSelectSize, m_txsStatusGossipMaxPeers);
    {
        for (auto tx : *_txs)//处理每笔tx  都随机选25%节点  发送
        {
            auto peers = m_syncStatus->filterPeers(//在_selectedPeers里选25%的可发送tx的节点（selectSize<selectedPeers.size()时）
                selectSize, _selectedPeers, [&](std::shared_ptr<SyncPeerStatus> _p) {
                    bool unsent = !tx->isTheNodeContainsTransaction(m_nodeId);
                    bool isSealer = _p->isSealer;
                    return isSealer && unsent && !tx->isTheNodeContainsTransaction(_p->nodeId);
                });
            if (peers.size() == 0)//都不可发
            {
                continue;
            }
            tx->appendNodeListContainTransaction(peers);//在交易中记录拥有此tx的节点
            tx->appendNodeContainsTransaction(m_nodeId);
            for (auto const& peer : peers)
            {
                if (!m_txsHash->count(peer))
                {
                    m_txsHash->insert(
                        std::make_pair(peer, std::make_shared<std::set<dev::h256>>()));
                }
                (*m_txsHash)[peer]->insert(tx->sha3());//记录需要发送本tx的nodes->m_txsHash
            }
        }//for
    }
    auto blockNumber = m_blockChain->number();
    for (auto const& it : *m_txsHash)//给选出的每个peer发txs hash
    {
        std::shared_ptr<SyncTxsStatusPacket> txsStatusPacket =
            std::make_shared<SyncTxsStatusPacket>();
        if (it.second->size() == 0)
        {
            continue;
        }
        txsStatusPacket->encode(blockNumber, it.second);//为什么需要区块号？？？  将区块高度和txs编码
        auto p2pMsg = txsStatusPacket->toMessage(m_protocolId);
        m_service->asyncSendMessageByNodeID(it.first, p2pMsg, CallbackFuncWithSession(), Options());
        SYNC_LOG(DEBUG) << LOG_BADGE("Tx") << LOG_DESC("Send transaction status to peer")
                        << LOG_KV("txNum", it.second->size())
                        << LOG_KV("toNode", it.first.abridged())
                        << LOG_KV("messageSize(B)", p2pMsg->length());
    }
    m_txsHash->clear();//发完清空
}

void SyncTransaction::updateNeedMaintainTransactions(bool const& _needMaintainTxs)
{
    if (_needMaintainTxs != m_needMaintainTransactions)//两种情况会发生
    {
        // changed from sealer/observer to free-node   1、节点退出群组
        if (m_needMaintainTransactions)
        {
            SYNC_LOG(DEBUG) << LOG_DESC(
                                   "updateNeedMaintainTransactions: node changed from "
                                   "sealer/observer to free-node, freshTxsStatus")
                            << LOG_KV("isSealerOrObserver", _needMaintainTxs)
                            << LOG_KV("isSealerOrObserverBeforeUpdate", m_needMaintainTransactions);
            m_txPool->freshTxsStatus();//对交易池中的 remain tx 作处理，使得加入群组后可重新同步remain tx
        }
        else//2、节点加入群组   初始启动走这条
        {
            SYNC_LOG(DEBUG) << LOG_DESC(
                                   "updateNeedMaintainTransactions: node changed from free-node to "
                                   "sealer/observer, noteNewTransactions")
                            << LOG_KV("isSealerOrObserver", _needMaintainTxs)
                            << LOG_KV("isSealerOrObserverBeforeUpdate", m_needMaintainTransactions);
            // changed from free-node into sealer/observer   标记有新交易       tx队列倒入交易池，所有标记有新的交易？
            noteNewTransactions();
        }
        m_needMaintainTransactions = _needMaintainTxs;
    }
}