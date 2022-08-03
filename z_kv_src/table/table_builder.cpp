#include "table_builder.h"

#include "../db/comparator.h"
#include "../logger/log.h"
#include "../utils/codec.h"
#include "../utils/crc32.h"
#include "footer.h"
#include "table_options.h"
namespace z_kv {
using namespace util;
//sst文件构建器
TableBuilder::TableBuilder(const Options& options, FileWriter* file_handler)
    : options_(options),
      index_options_(options),
      data_block_builder_(&options_),
      index_block_builder_(&index_options_),
      filter_block_builder_(options_) {
  //indexblock的分片大小为1（不分片，不压缩）
  index_options_.block_restart_interval = 1;
  file_handler_ = file_handler;
}
//向sst文件中添加KV，缓冲区达到要求就调用落盘
void TableBuilder::Add(const std::string_view& key,
                       const std::string_view& value) {
  if (key.empty()) {
    return;
  }
  //向indexblock的buffer添加一条数据（在一个datablock完全落盘之后才会触发此操作）
  if (need_create_index_block_ && options_.comparator) {
    //pre_block_last_key_传出为pre_block_last_key_,和key之间的最短串
    // index中key做了优化，尽可能短
    options_.comparator->FindShortest(pre_block_last_key_, key);
    std::string output;
    // index中的value保存的是当前key在block中的偏移量和对应的block大小
    index_block_offset_size_builder_.Encode(pre_block_offset_size_, output);
    index_block_builder_.Add(pre_block_last_key_, output);
    need_create_index_block_ = false;
  }
  // 把key添加到布隆过滤器的buffer中，为最后构建布隆过滤器做准备(整个sst就构建一个)
  if (filter_block_builder_.Availabe()) {
    filter_block_builder_.Add(key);
  }

  pre_block_last_key_ = key;//用当前key覆盖前一个key
  ++entry_count_;//数据计数加一
  // 写入data block
  data_block_builder_.Add(key, value);
  // 如果block的大小达到要求，进行一个刷盘操作
  if (data_block_builder_.CurrentSize() >= options_.block_size) {
    Flush();
  }
}
//将datablock落盘
void TableBuilder::Flush() {
  if (data_block_builder_.CurrentSize() == 0) {
    return;
  }
  //把分片信息追加到datablock的buffer，然后落盘，最后重置datablock
  WriteDataBlock(data_block_builder_, pre_block_offset_size_);
  // 如果写入数据成功
  if (status_ == Status::kSuccess) {
    //在下一轮循环中时，就需要更新我们的index block数据
    need_create_index_block_ = true;
    // 对于批量写缓冲区剩余的数据需要手动进行刷盘，至此一个block才能保证全部落盘
    status_ = file_handler_->FlushBuffer();
  }
}
//把分片信息追加到datablock的buffer，然后落盘，返回位置信息，最后重置datablock
void TableBuilder::WriteDataBlock(DataBlockBuilder& data_block_builder,
                                  OffSetSize& offset_size) {
  // restart_pointer（分片信息）加在数据后面
  data_block_builder.Finish();
  // 将所有的record数据打包
  const std::string& data = data_block_builder.Data();
  WriteBytesBlock(data, options_.block_compress_type, offset_size);
  // 这里直接调用reset操作，主要目的是比较方便，创建一个空对象直接替换也行
  data_block_builder.Reset();
}
//把打包好的数据写入文件，并追加压缩类型和校验,同时使用第三个参数传出本此写入的block的位置信息（最终落盘）
void TableBuilder::WriteBytesBlock(const std::string& datas,
                                   BlockCompressType block_compress_type,
                                   OffSetSize& offset_size) {
  std::string compress_data = datas;
  bool compress_success = false;
  //确定压缩类型
  BlockCompressType type = block_compress_type;
  switch (block_compress_type) {
    case kSnappyCompression: {
      compress_data = datas;
      compress_success = true;
    } break;
    default:
      type = kNonCompress;
      break;
  }

  offset_size.offset = block_offset_;
  offset_size.length = datas.size();
  // 在sst中追加我们的block数据
  status_ = file_handler_->Append(datas.data(), datas.size());
  char trailer[kBlockTrailerSize];//一位的压缩类型+4位的循环校验
  //写入压缩类型
  trailer[0] = static_cast<uint8_t>(type);
  //生成crc校验码
  uint32_t crc = crc32::Value(datas.data(), datas.size());
  crc = crc32::Extend(crc, trailer, 1);  // Extend crc to cover block type
  //定长编码
  EncodeFixed32(trailer + 1, crc32::Mask(crc));
  //追加后缀数据
  status_ = file_handler_->Append(trailer, kBlockTrailerSize);
  if (status_ == Status::kSuccess) {
    //更新写入偏移
    block_offset_ += offset_size.length + kBlockTrailerSize;
  }
}
//结束一个sst文件（写入filter_block meta_filter index_block和最后footer）
void TableBuilder::Finish() {
  if (!Success()) {
    return;
  }
  // 把data buffer中剩余的数据刷到磁盘
  Flush();
  OffSetSize filter_block_offset, meta_filter_block_offset, index_block_offset;
  // 开始构建filter_block和meta_filter_block_offset(这部分其实保存到footer中)
  //写filter_block数据
  if (filter_block_builder_.Availabe()) {
    // 这部分写的是filter即布隆过滤器部分数据
    filter_block_builder_.Finish();
    const auto& filter_block_data = filter_block_builder_.Data();

    // 这部分不需要进行压缩，所以直接调用WriteBytesBlock函数
    WriteBytesBlock(filter_block_data, BlockCompressType::kNonCompress,
                    filter_block_offset);
    // 该部分是获取布隆过滤器部分数据在整个sst中的位置，然后将这部分数据写入到sst中
    // 这部分目的是针对不同的块可以使用不同的filter_policy
    DataBlockBuilder meta_filter_block(&options_);//构建block
    OffsetBuilder filter_block_offset_builder;//block位置属性编解码
    std::string handle_encoding_str;
    filter_block_offset_builder.Encode(filter_block_offset,
                                       handle_encoding_str);
    //meta_filter_block只有一条kv数据
    //加到缓冲区
    meta_filter_block.Add(options_.filter_policy->Name(), handle_encoding_str);
    //落盘
    WriteDataBlock(meta_filter_block, meta_filter_block_offset);
  }
  // 处理index_block
  if (need_create_index_block_ && options_.comparator) {
    // 最后一个key不做优化了，直接使用(leveldb中是FindShortSuccessor(std::string*
    // key)函数)
    // index中的value保存的是当前key在block中的偏移量和对应的block大小
    std::string output;
    index_block_offset_size_builder_.Encode(pre_block_offset_size_, output);
    index_block_builder_.Add(pre_block_last_key_, output);
    need_create_index_block_ = false;
  }
  //index_block落盘
  WriteDataBlock(_builder_, index_block_offset);
  Footer footer;//最后一块定长40个字节
  footer.SetFilterBlockMetaData(meta_filter_block_offset);
  footer.SetIndexBlockMetaData(index_block_offset);
  std::string footer_output;
  footer.EncodeTo(&footer_output);
  //直接写入文件
  file_handler_->Append(footer_output.data(), footer_output.size());
  block_offset_ += footer_output.size();
  file_handler_->Close();
}
}  // namespace corekv
