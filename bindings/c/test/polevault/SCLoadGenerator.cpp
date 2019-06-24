#include "SCLoadGenerator.h"
#include "ConsumerAdapterUtils.h"
#include <boost/lexical_cast.hpp>

using boost::asio::ip::tcp;
using namespace std;
using namespace ConsAdapter::serialization;

UID SCLoadGenerator::uidFuzz() { return UID(rand64(), rand64()); }

int SCLoadGenerator::endpointFuzz() {
  int ep = rand();
  while (epsWaitingForReply.find(ep) != epsWaitingForReply.end() ||
         epToVerifyRangesWaitingForResponse.find(ep) !=
             epToVerifyRangesWaitingForResponse.end()) {
    ep = rand();
  }
  epsWaitingForReply.insert(ep);
  return ep;
}

GlobalVersion SCLoadGenerator::globalVersionFuzz() {
  return GlobalVersion(rand64(), rand64(), rand());
}

ReplicatorState SCLoadGenerator::registerReplicatorState() {
  registeredUID = uidFuzz();
  return replicatorStateFuzz();
}

ReplicatorState SCLoadGenerator::replicatorStateFuzz() {
  return ReplicatorState(globalVersionFuzz(), registeredUID);
}

bool SCLoadGenerator::keyInVerifyRanges(int64_t keyIdx) {
  for (auto krInfo : verifyRangesWaitingForResponse) {
    if (keyIdx >= krInfo.keyRange.first && keyIdx <= krInfo.keyRange.second) {
      return true;
    }
  }
  return false;
}

// will never return a key in a range waiting for verification
int64_t SCLoadGenerator::keyIdxFuzz() {
  int64_t keyIdx;
  do {
    keyIdx = rand64() % keyRange;
  } while (keyInVerifyRanges(keyIdx));
  return keyIdx;
}

std::string SCLoadGenerator::keyFromKeyIdx(uint64_t index) {
  return fmt::format("{}{:09}", keyPrefix, index);
}

/*
int64_t SCLoadGenerator::keyIdxFromKey(std::string key) {
  auto idxStr = key.substr(keyPrefix.size());
  trace->info("SCGen get key id: {}" idxStr);
  return boost::lexical_cast<int64_t>(key.substr(keyPrefix.size()));
}
*/
std::string SCLoadGenerator::valueFuzz() {
  int size = rand64() % valueRange;
  int dataPtr = rand64() % (100000 - size);
  auto retStr = std::string((const char *)dataFuzz.get(), size);
  log.trace(LogLevel::Debug, "SCLoadGenerateValue", {{"val", retStr}});
  return retStr;
}

int SCLoadGenerator::getVerifyRangesReq(
    flatbuffers::FlatBufferBuilder &serializer) {
  int count = rand64() % rangeCountMax + 1;
  auto endpoint = endpointFuzz();
  std::vector<flatbuffers::Offset<KeyRange>> keyRangeVector;
  std::vector<unsigned> checksumsVector;

  for (int i = 0; i < count; i++) {
    if (verifyRangesWaitingToSend.empty()) {
      break;
    }
    auto krInfoIt = verifyRangesWaitingToSend.begin();
    auto k1 = serializer.CreateString(keyFromKeyIdx(krInfoIt->keyRange.first));
    auto k2 = serializer.CreateString(keyFromKeyIdx(krInfoIt->keyRange.second));
    auto kr = CreateKeyRange(serializer, k1, k2);
    keyRangeVector.push_back(kr);
    checksumsVector.push_back(krInfoIt->checksum);
    log.trace(LogLevel::Debug, "SCLoadGenVerifyReq",
              {{"kr", krInfoIt->toStr()}});

    epToVerifyRangesWaitingForResponse[endpoint].push_back(*krInfoIt);
    verifyRangesWaitingToSend.erase(krInfoIt);
  }
  epToStats[endpoint].ranges = count;
  // create request object
  auto checksums = serializer.CreateVector(checksumsVector);
  auto keyRanges = serializer.CreateVector(keyRangeVector);
  auto repState = replicatorStateFuzz();
  auto verifyReq =
      CreateVerifyRangeReq(serializer, &repState, keyRanges, checksums);
  auto req = CreateConsumerAdapterRequest(serializer, Request_VerifyRangeReq,
                                          verifyReq.Union(), endpoint);
  serializer.Finish(req);
  epToStats[endpoint].type = MessageBufferType::T_VerifyRangeReq;
  return endpoint;
}

MessageStats SCLoadGenerator::waitingEPGotReply(int endpoint) {
  if (epsWaitingForReply.find(endpoint) == epsWaitingForReply.end()) {
    log.trace(LogLevel::Error, "SCLoadGenEPWaitingForReplyNotFound",
              {{"endpoint", STR(endpoint)}});
    assert(0);
  }
  epsWaitingForReply.erase(endpoint);
  if (epToVerifyRangeWaitingForPush.find(endpoint) !=
      epToVerifyRangeWaitingForPush.end()) {
    auto vrInfo = epToVerifyRangeWaitingForPush[endpoint];
    log.trace("SCLoadGenEPWaitingForReply_VerifyWaitingForPushFinished",
              {{"endpoint", STR(endpoint)}, {"kr", vrInfo.toStr()}});
    verifyRangesWaitingToSend.insert(vrInfo);
    epToVerifyRangeWaitingForPush.erase(endpoint);
  }
  if (epsWaitingForSet.find(endpoint) != epsWaitingForSet.end()) {
    log.trace("SCLoadGenEPWaitingForReply_SetRepStateFinished",
              {{"endpoint", STR(endpoint)}});
    epsWaitingForSet.erase(endpoint);
  }
  auto epStats = epToStats[endpoint];
  if (epStats.type != MessageBufferType::T_VerifyRangeReq) {
    epToStats.erase(endpoint);
  }
  return epStats;
}

MessageStats SCLoadGenerator::waitingEPGotVerifyFinish(int endpoint) {

  if (epToVerifyRangesWaitingForResponse.find(endpoint) ==
      epToVerifyRangesWaitingForResponse.end()) {
    log.trace(LogLevel::Error, "SCLoadGenEPWaitingForVerifyNotFound",
              {{"endpoint", STR(endpoint)}});
    assert(0);
  }
  for (auto krInfo : epToVerifyRangesWaitingForResponse[endpoint]) {
    log.trace("SCLoadGenEPWaitingForVerifyFinished",
              {{"endpoint", STR(endpoint)}, {"kr", krInfo.toStr()}});
    verifyRangesWaitingForResponse.erase(krInfo);
  }
  epToVerifyRangesWaitingForResponse.erase(endpoint);
  auto epStats = epToStats[endpoint];
  epToStats.erase(endpoint);
  return epStats;
}

int SCLoadGenerator::getGetRepStateReq(
    flatbuffers::FlatBufferBuilder &serializer) {
  auto repState = replicatorStateFuzz();
  auto endpoint = endpointFuzz();
  log.trace("SCLoadGenerateRepStateReq", {{"state", printObj(repState)}});
  auto repReq = CreateGetReplicatorStateReq(serializer, &repState);
  auto req = CreateConsumerAdapterRequest(
      serializer, Request_GetReplicatorStateReq, repReq.Union(), endpoint);
  serializer.Finish(req);
  epToStats[endpoint].type = MessageBufferType::T_GetReplicatorStateReq;
  return endpoint;
}

int SCLoadGenerator::getSetRepStateReq(
    flatbuffers::FlatBufferBuilder &serializer) {
  auto repState = registerReplicatorState();
  auto endpoint = endpointFuzz();
  log.trace("SCLoadGenerateRepStateReq",
            {{"state", printObj(repState)}, {"endpoint", STR(endpoint)}});
  auto setReq = CreateSetReplicatorStateReq(serializer, &repState);
  auto req = CreateConsumerAdapterRequest(
      serializer, Request_SetReplicatorStateReq, setReq.Union(), endpoint);
  serializer.Finish(req);
  epToStats[endpoint].type = MessageBufferType::T_SetReplicatorStateReq;
  epsWaitingForSet.insert(endpoint);
  return endpoint;
}

int SCLoadGenerator::getPushBatchReq(
    flatbuffers::FlatBufferBuilder &serializer) {
  std::vector<flatbuffers::Offset<Mutation>> mutationsVector;
  bool doVerify = (rand64() % 100 < 15);
  auto endpoint = endpointFuzz();
  if (verifyRangesWaitingForResponse.size() < maxOutstandingVerifyRanges &&
      doVerify) {

    int count = rand64() % maxKeyRangeSize + 1;
    // verify this batch
    // this batch will only contain one range
    auto keyIdxStart = keyIdxFuzz();
    auto keyIdx = keyIdxStart;
    Crc32 crc;
    int checksum = 0;
    for (int i = 0; i < count; i++) {
      if (keyInVerifyRanges(keyIdxStart + i)) {
        break;
      }
      keyIdx = keyIdxStart + i;
      auto kStr = keyFromKeyIdx(keyIdx);
      auto vStr = valueFuzz();
      auto k = serializer.CreateString(kStr);
      auto v = serializer.CreateString(vStr);
      auto m = CreateMutation(serializer, 0, k, v);
      mutationsVector.push_back(m);
      log.trace(LogLevel::Debug, "SCLoadGenerateMutAddToVerifyQueue",
                {{"key", kStr}});
      //      trace->debug("SCGen add push mut to verify queue: key:{}
      //      val:{}", kStr,
      //             vStr);
      crc.block(kStr.c_str(), kStr.size());
      crc.block(vStr.c_str(), vStr.size());

      epToStats[endpoint].bytes += kStr.size();
      epToStats[endpoint].bytes += vStr.size();
    }
    checksum = crc.sum();
    VerifyRangeInfo vrInfo(keyIdxStart, keyIdx, checksum);
    log.trace("SCLoadGenerateAddBatchToVerifyQueue",
              {{"verifyInfo", vrInfo.toStr()}});
    verifyRangesWaitingForResponse.insert(vrInfo);
    epToVerifyRangeWaitingForPush[endpoint] = vrInfo;

  } else {

    int count = rand64() % mutationCountMax + 1;
    for (int i = 0; i < count; i++) {
      auto kStr = keyFromKeyIdx(keyIdxFuzz());
      auto vStr = valueFuzz();
      auto k = serializer.CreateString(kStr);
      auto v = serializer.CreateString(vStr);
      auto m = CreateMutation(serializer, 0, k, v);
      mutationsVector.push_back(m);
      epToStats[endpoint].bytes += kStr.size();
      epToStats[endpoint].bytes += vStr.size();

      log.trace(LogLevel::Debug, "SCLoadGenerateMutAddToBatch",
                {{"key", kStr}});
      // trace->debug("SCGen add push to batch keyValue({}:{}) ", kStr, vStr);
    }
  }
  auto mutations = serializer.CreateVector(mutationsVector);

  auto repState = replicatorStateFuzz();
  auto pushBatchReq = CreatePushBatchReq(serializer, &repState, mutations);
  auto req = CreateConsumerAdapterRequest(serializer, Request_PushBatchReq,
                                          pushBatchReq.Union(), endpoint);
  serializer.Finish(req);
  epToStats[endpoint].type = MessageBufferType::T_PushBatchReq;
  return endpoint;
}

void SCLoadGenerator::updateEndpointSendTime(int endpoint) {
  epToStats[endpoint].sendTS = std::chrono::system_clock::now();
}

void SCLoadGenerator::init(int kRange, int vRange, int mCountMax, int mKRSize,
                           int rCountMax, int maxOSVR) {
  keyRange = kRange;
  valueRange = vRange;
  mutationCountMax = mCountMax;
  maxKeyRangeSize = mKRSize;
  rangeCountMax = rCountMax;
  maxOutstandingVerifyRanges = maxOSVR;
  int dataSize = 100000 / sizeof(uint64_t);
  dataFuzz.reset(new uint64_t[dataSize]);
  for (int i = 0; i < dataSize; i++) {
    dataFuzz[i] = rand64();
  }
}

void SCLoadGenerator::printMutVector(
    const flatbuffers::Vector<flatbuffers::Offset<Mutation>> *mutations) {
  for (auto i = 0; i < mutations->Length(); i++) {
    log.trace("PrintMutation",
              {{"mutation", printObj(*mutations->Get(i))},
               {"size", STR(mutations->Get(i)->param1()->str().size())}});
  }
}
void SCLoadGenerator::printRangesVector(
    const flatbuffers::Vector<flatbuffers::Offset<KeyRange>> *keyRanges) {
  for (auto i = 0; i < keyRanges->Length(); i++) {
    log.trace("PrintKeyRanges", {{"keyRange", printObj(*keyRanges->Get(i))}});
  }
}
