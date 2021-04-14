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
/** @file LedgerInitializer.h
 *  @author chaychen
 *  @modify first draft
 *  @date 20181022
 */

#include "LedgerInitializer.h"
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>

using namespace dev;
using namespace std;
using namespace dev::initializer;
using namespace dev::ledger;

void LedgerInitializer::initConfig(boost::property_tree::ptree const& _pt)
{
    INITIALIZER_LOG(INFO) << LOG_BADGE("LedgerInitializer") << LOG_DESC("initConfig");

    m_groupDataDir = _pt.get<string>("group.group_data_path", "data/");
    m_groupConfigPath = _pt.get<string>("group.group_config_path", "conf/");
    m_ledgerManager = make_shared<LedgerManager>();
    assert(m_p2pService);
    assert(m_groupConfigPath.length() != 0);

    g_BCOSConfig.setConfDir(m_groupConfigPath);
    g_BCOSConfig.setDataDir(m_groupDataDir);

    initLedgers();
}


bool LedgerInitializer::initLedgerByGroupID(dev::GROUP_ID const& _groupId)
{
    namespace fs = boost::filesystem;

    fs::path genesisConfFilePath(
        m_groupConfigPath + fs::path::separator + "group." + to_string(_groupId) + ".genesis");
    if (!fs::exists(genesisConfFilePath.string()))
    {
        BOOST_THROW_EXCEPTION(GenesisConfNotFound());
    }

    fs::path groupConfFilePath(
        m_groupConfigPath + fs::path::separator + "group." + to_string(_groupId) + ".ini");
    if (!fs::exists(groupConfFilePath.string()))
    {
        BOOST_THROW_EXCEPTION(GroupConfNotFound());
    }

    return initLedger(_groupId, m_groupDataDir, genesisConfFilePath.string());
}


vector<dev::GROUP_ID> LedgerInitializer::initLedgers()
{
    vector<dev::GROUP_ID> newGroupIDList;//函数返回值  没用上
    try
    {
        newGroupIDList = foreachLedgerConfigure(m_groupConfigPath, [&](dev::GROUP_ID const&
                                                                           _groupID,//从.genesis读取的群组id
                                                                       const string&
                                                                           _configFileName) {//每个   .genesis文件的路径名（包括路径和文件名）？？？
            try
            {
                // skip existing group    账本 ID 是否已存在
                if (m_ledgerManager->isLedgerExist(_groupID))
                {
                    return false;
                }

                if (m_ledgerManager->isLedgerHaltedBefore(_groupID))//账本之前是否被暂停？
                {
                    return false;
                }

                bool succ = initLedger(_groupID, m_groupDataDir, _configFileName);
                if (!succ)
                {
                    INITIALIZER_LOG(ERROR)
                        << LOG_BADGE("LedgerInitializer") << LOG_DESC("initSingleGroup failed")
                        << LOG_KV("configFile", _configFileName);
                    ERROR_OUTPUT << LOG_BADGE("LedgerInitializer")
                                 << LOG_DESC("initSingleGroup failed")
                                 << LOG_KV("configFile", _configFileName) << endl;
                    BOOST_THROW_EXCEPTION(InitLedgerConfigFailed());
                    return false;
                }
                LOG(INFO) << LOG_BADGE("LedgerInitializer init group succ")
                          << LOG_KV("groupID", _groupID);
                return true;
            }
            catch (UnknownGroupStatus& e)
            {
                INITIALIZER_LOG(ERROR)
                    << LOG_BADGE("LedgerInitializer")
                    << LOG_DESC(
                           "Invalid group status, please check `.group_status` file of the group")
                    << LOG_KV("groupID", _groupID);
                return false;
            }
            catch (exception& e)
            {
                // Note: This exception is thrown by the lamda expression,
                //        and the outer function cannot catch the specific error
                ERROR_OUTPUT << LOG_BADGE("LedgerInitializer") << LOG_DESC("initLedger failed")
                             << LOG_KV("errorInfo", boost::diagnostic_information(e));
                BOOST_THROW_EXCEPTION(e);
            }
        });
    }
    catch (exception& e)
    {
        INITIALIZER_LOG(ERROR) << LOG_BADGE("LedgerInitializer") << LOG_DESC("initLedger failed")
                               << LOG_KV("EINFO", boost::diagnostic_information(e));
        ERROR_OUTPUT << LOG_BADGE("LedgerInitializer") << LOG_DESC("initLedger failed")
                     << LOG_KV("EINFO", boost::diagnostic_information(e)) << endl;
        BOOST_THROW_EXCEPTION(e);
    }
    return newGroupIDList;
}

vector<dev::GROUP_ID> LedgerInitializer::foreachLedgerConfigure(
    const string& _groupConfigPath, function<bool(dev::GROUP_ID const&, const string&)> _f)
{
    namespace fs = boost::filesystem;
    LOG(INFO) << LOG_BADGE("LedgerInitializer") << LOG_KV("groupConfigPath", _groupConfigPath);
    fs::path path(_groupConfigPath);
    vector<dev::GROUP_ID> reachList;
    if (fs::is_directory(path))
    {
        fs::directory_iterator endIter;
        for (fs::directory_iterator iter(path); iter != endIter; iter++)//扫描 群组配置dir中的 所有 .genesis文件，文件个数代表群组个数？
        {//tert代表什么？？
            if (fs::extension(*iter) == ".genesis")
            {
                // parse group id
                boost::property_tree::ptree pt;
                boost::property_tree::read_ini(iter->path().string(), pt);
                auto groupID = pt.get<int>("group.id", 0);

                // check groupID format
                if (groupID <= 0 || groupID > maxGroupID)
                {
                    INITIALIZER_LOG(ERROR)
                        << LOG_BADGE("LedgerInitializer") << LOG_DESC("groupID invalid")
                        << LOG_KV("groupID", groupID)
                        << LOG_KV("configFile", iter->path().string());
                    continue;
                }

                if (_f(groupID, iter->path().string()))
                {
                    reachList.emplace_back(groupID);
                }
            }
        }
    }
    return reachList;
}

void LedgerInitializer::startMoreLedger()
{
    assert(m_groupConfigPath.length() != 0);
    vector<dev::GROUP_ID> newGroupIDList = initLedgers();

    for (auto groupID : newGroupIDList)
    {
        m_ledgerManager->startByGroupID(groupID);
    }
}

void LedgerInitializer::reloadSDKAllowList()
{
    auto groupList = m_ledgerManager->getGroupList();
    for (auto const& group : groupList)
    {
        auto ledger = m_ledgerManager->ledger(group);
        if (!ledger)
        {
            continue;
        }
        ledger->reloadSDKAllowList();
    }
}

bool LedgerInitializer::initLedger(
    dev::GROUP_ID const& _groupId, std::string const& _dataDir, std::string const& _configFileName)
{
    if (m_ledgerManager->isLedgerExist(_groupId))//多此一举了
    {
        // Already initialized
        return true;
    }
    INITIALIZER_LOG(INFO) << "[initSingleLedger] [GroupId], LedgerConstructor:  "
                          << std::to_string(_groupId) << LOG_KV("configFileName", _configFileName)
                          << LOG_KV("dataDir", _dataDir);
    auto ledger = std::make_shared<Ledger>(m_p2pService, _groupId, m_keyPair);
    ledger->setChannelRPCServer(m_channelRPCServer);
    auto ledgerParams = std::make_shared<LedgerParam>();
    ledgerParams->init(_configFileName, _dataDir);//解析每个群组的配置文件；群组的配置文件目录（可多个群组配置文件，放在一起的）  群组的data目录（可多个群组的data，分开的）
    bool succ = ledger->initLedger(ledgerParams);
    if (!succ)
    {
        return false;
    }

    m_ledgerManager->insertLedger(_groupId, ledger);
    return true;
}
