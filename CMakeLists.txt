#------------------------------------------------------------------------------
# Top-level CMake file for FISCO-BCOS.
# ------------------------------------------------------------------------------
# This file is part of FISCO-BCOS.
#
# FISCO-BCOS is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# FISCO-BCOS is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with FISCO-BCOS.  If not, see <http://www.gnu.org/licenses/>
#
# (c) 2016-2018 fisco-dev contributors.
#------------------------------------------------------------------------------

cmake_minimum_required(VERSION 3.7)   #最低版本要求  如果小于，在cmake阶段就会报错
#cmake dir 就是 cmake目录  ，有一些脚本，cmake通过这个
set(FISCO_BCOS_CMAKE_DIR "${CMAKE_CURRENT_LIST_DIR}/cmake" CACHE PATH "The path to the cmake directory")
#给变量CMAKE_MODULE_PATH 添加一个 cmake 脚本 的 路径  接下来include那些脚本的时候，就能找到这些脚本，按照顺序执行
list(APPEND CMAKE_MODULE_PATH ${FISCO_BCOS_CMAKE_DIR})
#macos最小的支持的版本
set(CMAKE_OSX_DEPLOYMENT_TARGET "10.13" CACHE STRING "Minimum OS X deployment version")

project(FISCO-BCOS VERSION "2.6.0")#当前版本
# Suffix like "-rc1" e.t.c. to append to versions wherever needed.
set(VERSION_SUFFIX "")

find_package(Git QUIET)#获取依赖时检查有没有安装git
if(NOT GIT_FOUND)
  message(FATAL_ERROR "Please install git")
endif()

set(CMAKE_SKIP_INSTALL_ALL_DEPENDENCY ON)

# basic setting
include(Options)#cmake目录中的option脚本    把cmake目录中写的脚本逐个包含进来
include(CompilerSettings)
include(Dependencies)
configure_project()

# install dependencies
include(ProjectTBB)
include(ProjectSnappy)
include(ProjectLevelDB)
include(ProjectRocksDB)
include(ProjectMHD)
include(ProjectCryptopp)
include(ProjectSecp256k1)
include(ProjectJsonCpp)
include(ProjectJsonRpcCpp)
include(ProjectTASSL)
include(ProjectBoost)#怎么编译boost，怎么编译成库，头文件放到指定路径   include后cmake会帮忙作这些事情
include(ProjectLibzdb)
include(ProjectTCMalloc)
include(ProjectLibFF)
include(ProjectPaillier)
include(ProjectGroupSig)
include(ProjectEVMC)
include(ProjectVRF)
include(ProjectEVMONE)

include_directories("${CMAKE_SOURCE_DIR}")

add_subdirectory(libchannelserver)#add每个模块
add_subdirectory(libdevcore)
add_subdirectory(libdevcrypto)
add_subdirectory(libethcore)
# add_subdirectory(libinterpreter)
add_subdirectory(libstat)
add_subdirectory(libflowlimit)
add_subdirectory(libtxpool)
add_subdirectory(libstorage)
add_subdirectory(libprecompiled)
add_subdirectory(libnetwork)
add_subdirectory(libp2p)
add_subdirectory(libexecutive)
add_subdirectory(libmptstate)
add_subdirectory(libblockverifier)
add_subdirectory(libstoragestate)
add_subdirectory(libblockchain)
add_subdirectory(libsync)
add_subdirectory(libconsensus)
add_subdirectory(libledger)
add_subdirectory(librpc)
add_subdirectory(libinitializer)
add_subdirectory(libsecurity)
add_subdirectory(libeventfilter)
# generate executable binary fisco-bcos  生成一个编译的有顺序的文件，make根据生成的文件编译
add_subdirectory(fisco-bcos)


if (TESTS)
    enable_testing()
    set(CTEST_OUTPUT_ON_FAILURE TRUE)#单测 时 设置环境变量  怎么把错误输出出来
    add_subdirectory(test)
endif()

find_package(Doxygen QUIET)#用doxygen生成一个具体的源码文档
if(DOXYGEN_FOUND)
# Requirements: doxygen graphviz
  set(doxyfile_in ${CMAKE_CURRENT_SOURCE_DIR}/.Doxyfile.in)
  set(doxyfile ${CMAKE_CURRENT_BINARY_DIR}/Doxyfile)
  configure_file(${doxyfile_in} ${doxyfile} @ONLY)
# Add doc target
add_custom_target(doc COMMAND ${DOXYGEN_EXECUTABLE} ${doxyfile}
                       WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                       COMMENT "Generating documentation with Doxygen..." VERBATIM)
elseif()
  message(WARNING "Doxygen is needed to build the documentation. Please install doxygen and graphviz")
endif()
