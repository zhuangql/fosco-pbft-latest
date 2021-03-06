/*
    This file is part of cpp-ethereum.

    cpp-ethereum is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    cpp-ethereum is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with cpp-ethereum.  If not, see <http://www.gnu.org/licenses/>.
*/
/** @file Common.h
 * @author Gav Wood <i@gavwood.com>
 * @date 2014
 *
 * @author wheatli
 * @date 2018.8.27
 * @modify add asAddress and fromAddress
 *
 * @author chaychen
 * @date 2018.9.4
 * @modify remove the special contract for block hash storage defined in EIP96
 *
 * Ethereum-specific data structures & algorithms.
 */

#pragma once

#include <libdevcore/Address.h>
#include <libdevcore/Common.h>
#include <libdevcore/Exceptions.h>
#include <libdevcore/FixedHash.h>
#include <stdint.h>

#include <functional>
#include <string>

namespace dev
{
class RLP;
class RLPStream;

namespace eth
{
/// Current protocol version.
extern const unsigned c_protocolVersion;

/// Current minor protocol version.
extern const unsigned c_minorProtocolVersion;

/// Current database version.
extern const unsigned c_databaseVersion;
extern const unsigned c_BlockFieldSize;
/// Convert the given string into an address.
Address toAddress(std::string const& _s);

/// The log bloom's size (2048-bit).
using LogBloom = h2048;

/// Many log blooms.
using LogBlooms = std::vector<LogBloom>;

// The various denominations; here for ease of use where needed within code.
static const u256 ether = exp10<18>();
static const u256 finney = exp10<15>();
static const u256 szabo = exp10<12>();
static const u256 shannon = exp10<9>();
static const u256 wei = exp10<0>();

using Nonce = h64;
using NonceKeyType = u256;
using BlockNumber = int64_t;
// the max blocknumber value
static const BlockNumber MAX_BLOCK_NUMBER = INT64_MAX;
// the max number of topic in event logd
static const uint32_t MAX_NUM_TOPIC_EVENT_LOG = 4;

enum class BlockPolarity
{
    Unknown,
    Dead,
    Live
};

class Transaction;

struct ImportRoute
{
    h256s deadBlocks;
    h256s liveBlocks;
    std::vector<Transaction> goodTranactions;
};

enum class ImportResult
{
    Success = 0,
    UnknownParent,
    FutureTimeKnown,
    FutureTimeUnknown,
    AlreadyInChain,
    AlreadyKnown,
    Malformed,
    BadChain,
    ZeroSignature,
    TransactionNonceCheckFail,
    TxPoolNonceCheckFail,
    TransactionPoolIsFull,
    InvalidChainId,
    InvalidGroupId,
    BlockLimitCheckFailed,
    NotBelongToTheGroup,
    TransactionRefused,
    OverGroupMemoryLimit
};

struct ImportRequirements
{
    using value = unsigned;
    enum
    {
        ValidSeal = 1,               ///< Validate seal
        TransactionBasic = 8,        ///< Check the basic structure of the transactions.
        TransactionSignatures = 32,  ///< Check the basic structure of the transactions.
        Parent = 64,                 ///< Check parent block header
        PostGenesis = 256,           ///< Require block to be non-genesis.
        CheckTransactions = TransactionBasic | TransactionSignatures,  ///< Check transaction
                                                                       ///< signatures.
        OutOfOrderChecks = ValidSeal | CheckTransactions,  ///< Do all checks that can be done
                                                           ///< independently of prior blocks having
                                                           ///< been imported.
        InOrderChecks = Parent,  ///< Do all checks that cannot be done independently
                                 ///< of prior blocks having been imported.
        Everything = ValidSeal | CheckTransactions | Parent,
        None = 0
    };
};

/// Super-duper signal mechanism. TODO: replace with somthing a bit heavier weight.
template <typename... Args>
class Signal
{
public:
    using Callback = std::function<void(Args...)>;

    class HandlerAux
    {
        friend class Signal;//嵌套类 可直接使用外层类成员，不需要对该成员的名字进行限定

    public:
        ~HandlerAux()
        {
            if (m_s)
                m_s->m_fire.erase(m_i);//删除f_fire的map，在析构handerAux的内存
        }
        void reset() { m_s = nullptr; }
        void fire(Args const&... _args) { m_h(_args...); }

    private:
        HandlerAux(unsigned _i, Signal* _s, Callback const& _h) : m_i(_i), m_s(_s), m_h(_h) {}//只有signal可以构造handleraux对象

        unsigned m_i = 0;//注册的hander序号；对应m_fire->first; 析构函数用的；
        Signal* m_s = nullptr;//signal的对象
        Callback m_h;//注册的其他对象的回调函数，调用注册对象的function
    };

    ~Signal()
    {
        for (auto const& h : m_fire)
            if (auto l = h.second.lock())
                l->reset();//这个调用的是智能指针的reset还是handerAux的reset？？？将map中所有的hander中的m_s置空，有啥用
    }

    std::shared_ptr<HandlerAux> add(Callback const& _h)//HandlerAux作为Signal的返回类型，在外层类作用域中，所以不用加限定符 Signal::
    {
        auto n = m_fire.empty() ? 0 : (m_fire.rbegin()->first + 1);
        auto h = std::shared_ptr<HandlerAux>(new HandlerAux(n, this, _h));//外层类的成员可以像使用其他类型成员一样使用嵌套类的名字（不需要限定符）
        m_fire[n] = h;//注册的hander要放在 hander所在内存中，因为如果那块内存回收就不该调用其hander了，所以signal用weakptr跟踪；
        return h;
    }

    void operator()(Args const&... _args)
    {
        for (auto const& f : valuesOf(m_fire))//返回m_fire的values（vector<typename Signal<Args...>::HandlerAux>）
            if (auto h = f.lock())
                h->fire(_args...);
    }

private:
    std::map<unsigned, std::weak_ptr<typename Signal::HandlerAux>> m_fire;//typename Signal::   没有应该也可以；嵌套类是外层类的一个类型成员
};

template <class... Args>
using Handler = std::shared_ptr<typename Signal<Args...>::HandlerAux>;//Signal<Args...>:: 不能省略，因为nested class在外层类作用域之外不可见

struct TransactionSkeleton
{
    bool creation = false;
    Address from;
    Address to;
    u256 value;
    bytes data;
    u256 nonce = Invalid256;
    u256 gas = Invalid256;
    u256 gasPrice = Invalid256;
    u256 blockLimit = Invalid256;
};


void badBlock(bytesConstRef _header, std::string const& _err);
inline void badBlock(bytes const& _header, std::string const& _err)
{
    badBlock(&_header, _err);
}

// TODO: move back into a mining subsystem and have it be accessible from Sealant only via a
// dynamic_cast.
/**
 * @brief Describes the progress of a mining operation.
 */
struct WorkingProgress
{
    //	MiningProgress& operator+=(MiningProgress const& _mp) { hashes += _mp.hashes; ms =
    // std::max(ms, _mp.ms); return *this; }
    uint64_t hashes = 0;  ///< Total number of hashes computed.
    uint64_t ms = 0;      ///< Total number of milliseconds of mining thus far.
    u256 rate() const { return ms == 0 ? 0 : hashes * 1000 / ms; }
};

/// Import transaction policy
enum class IfDropped
{
    Ignore,  ///< Don't import transaction that was previously dropped.
    Retry    ///< Import transaction even if it was dropped before.
};

// Convert from a 256-bit integer stack/memory entry into a 160-bit Address hash.
// Currently we just pull out the right (low-order in BE) 160-bits.
inline Address asAddress(u256 _item)
{
    return right160(h256(_item));
}

inline u256 fromAddress(Address _a)
{
    return (u160)_a;
}

}  // namespace eth
}  // namespace dev
