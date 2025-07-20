import hashlib
import heapq
import random
import json
from abc import ABC, abstractmethod


# 1. Symbol 接口定义
class Symbol(ABC):
    @abstractmethod
    def xor(self, other):
        pass

    @abstractmethod
    def hash(self):
        pass

# 2. 内容符号的具体实现
class ContentSymbol(Symbol):
    def __init__(self, data):
        if isinstance(data, str):
            self.data = data.encode('utf-8')
        elif isinstance(data, bytes):
            self.data = data
        else:
            self.data = str(data).encode('utf-8')

    def xor(self, other: 'ContentSymbol') -> 'ContentSymbol':
        # 确保两个字节串长度一致
        len1, len2 = len(self.data), len(other.data)
        if len1 < len2:
            d1 = self.data.ljust(len2, b'\0')
            d2 = other.data
        else:
            d1 = self.data
            d2 = other.data.ljust(len1, b'\0')
        
        res = bytes(a ^ b for a, b in zip(d1, d2))
        return ContentSymbol(res)

    def hash(self):
        return int(hashlib.sha256(self.data).hexdigest(), 16)

    def __repr__(self):
        return f"ContentSymbol('{self.data.decode('utf-8', errors='ignore')}')"

# 3. 数据结构
class HashedSymbol:
    def __init__(self, symbol):
        self.symbol = symbol
        self.hash = symbol.hash()

class CodedSymbol:
    def __init__(self):
        self.symbol = ContentSymbol(b'')
        self.hash = 0
        self.count = 0

    def apply(self, s, direction):
        self.symbol = self.symbol.xor(s.symbol)
        self.hash ^= s.hash
        self.count += direction
        return self

# 4. 随机映射
class RandomMapping:
    def __init__(self, seed, last_idx=0):
        self.state = seed
        self.last_idx = last_idx

    def next_index(self):
        # 简单的伪随机数生成器
        self.state = (self.state * 1103515245 + 12345) & 0xFFFFFFFFFFFFFFFF
        self.last_idx += 1 + (self.state % 10) # 增加随机步长
        return self.last_idx

# 5. 编码窗口
class CodingWindow:
    def __init__(self):
        self.symbols = []
        self.mappings = []
        self.queue = []
        self.next_idx = 0

    def add_symbol(self, symbol):
        hs = HashedSymbol(symbol)
        self.add_hashed_symbol(hs)

    def add_hashed_symbol(self, hs):
        self.add_hashed_symbol_with_mapping(hs, RandomMapping(hs.hash))

    def add_hashed_symbol_with_mapping(self, hs, m):
        self.symbols.append(hs)
        self.mappings.append(m)
        heapq.heappush(self.queue, (m.last_idx, len(self.symbols) - 1))

    def apply_window(self, cw, direction):
        if not self.queue:
            self.next_idx += 1
            return cw
        
        while self.queue and self.queue[0][0] == self.next_idx:
            _, source_idx = heapq.heappop(self.queue)
            cw = cw.apply(self.symbols[source_idx], direction)
            next_map_idx = self.mappings[source_idx].next_index()
            heapq.heappush(self.queue, (next_map_idx, source_idx))
        
        self.next_idx += 1
        return cw

    def reset(self):
        self.symbols = []
        self.mappings = []
        self.queue = []
        self.next_idx = 0

# 6. 编码器
class Encoder(CodingWindow):
    def produce_next_coded_symbol(self):
        return self.apply_window(CodedSymbol(), 1)

# 7. 解码器
class Decoder:
    def __init__(self):
        self.cs = []
        self.local = CodingWindow()
        self.window = CodingWindow()
        self.remote = CodingWindow()
        self.decodable = []
        self.decoded_count = 0

    def decoded(self):
        return self.decoded_count == len(self.cs)

    def add_symbol(self, s):
        self.window.add_symbol(s)

    def add_coded_symbol(self, c):
        c = self.window.apply_window(c, -1)
        c = self.remote.apply_window(c, -1)
        c = self.local.apply_window(c, 1)
        self.cs.append(c)
        if (c.count == 1 or c.count == -1) and (c.hash == c.symbol.hash()):
            self.decodable.append(len(self.cs) - 1)
        elif c.count == 0 and c.hash == 0:
            self.decodable.append(len(self.cs) - 1)

    def try_decode(self):
        while self.decodable:
            cidx = self.decodable.pop(0)
            c = self.cs[cidx]

            if c.count == 1:
                hs = HashedSymbol(c.symbol)
                m = self.apply_new_symbol(hs, -1)
                self.remote.add_hashed_symbol_with_mapping(hs, m)
                self.decoded_count += 1
            elif c.count == -1:
                hs = HashedSymbol(c.symbol)
                m = self.apply_new_symbol(hs, 1)
                self.local.add_hashed_symbol_with_mapping(hs, m)
                self.decoded_count += 1
            elif c.count == 0:
                self.decoded_count += 1

    def apply_new_symbol(self, t, direction):
        m = RandomMapping(t.hash)
        while m.last_idx < len(self.cs):
            cidx = m.last_idx
            self.cs[cidx] = self.cs[cidx].apply(t, direction)
            if (self.cs[cidx].count == 1 or self.cs[cidx].count == -1) and \
               (self.cs[cidx].hash == self.cs[cidx].symbol.hash()):
                if cidx not in self.decodable:
                    self.decodable.append(cidx)
            m.next_index()
        return m

# 8. Rateless IBLT 管理器
class RatelessIBLTManager:
    def encode(self, context_dict, num_symbols_multiplier=1.5):
        encoder = Encoder()
        symbols_to_encode = []
        for key, value in context_dict.items():
            # 处理bytes类型的值
            if isinstance(value, bytes):
                serializable_value = value.decode('utf-8')
            else:
                serializable_value = value
            
            # 将键值对序列化为符号
            symbol_data = json.dumps({key: serializable_value}, sort_keys=True)
            symbols_to_encode.append(ContentSymbol(symbol_data))
        
        for symbol in symbols_to_encode:
            encoder.add_symbol(symbol)

        num_symbols = int(len(symbols_to_encode) * num_symbols_multiplier)
        
        coded_symbols = []
        for _ in range(num_symbols):
            coded_symbols.append(encoder.produce_next_coded_symbol())
        
        return self.serialize_coded_symbols(coded_symbols)

    def decode(self, coded_symbols_serialized, local_context_dict):
        decoder = Decoder()
        for key, value in local_context_dict.items():
            symbol_data = json.dumps({key: value}, sort_keys=True)
            decoder.add_symbol(ContentSymbol(symbol_data))

        coded_symbols = self.deserialize_coded_symbols(coded_symbols_serialized)
        for cs in coded_symbols:
            decoder.add_coded_symbol(cs)
        
        decoder.try_decode()

        added = {}
        removed = set()
        updated = {}

        remote_symbols = [json.loads(s.symbol.data.decode('utf-8')) for s in decoder.remote.symbols]
        local_symbols = [json.loads(s.symbol.data.decode('utf-8')) for s in decoder.local.symbols]

        # 远程多出来的，是新增或更新
        for item in remote_symbols:
            key = list(item.keys())[0]
            value = list(item.values())[0]
            if key in local_context_dict:
                updated[key] = value
            else:
                added[key] = value

        # 本地多出来的，是删除
        for item in local_symbols:
            key = list(item.keys())[0]
            removed.add(key)

        return added, removed, updated

    def serialize_coded_symbols(self, coded_symbols):
        return json.dumps([
            {
                "symbol": cs.symbol.data.decode('latin1'), # 使用 'latin1' 编码以避免Unicode错误
                "hash": cs.hash,
                "count": cs.count
            } for cs in coded_symbols
        ])

    def deserialize_coded_symbols(self, serialized_data):
        data = json.loads(serialized_data)
        coded_symbols = []
        for item in data:
            cs = CodedSymbol()
            cs.symbol = ContentSymbol(item['symbol'].encode('latin1'))
            cs.hash = item['hash']
            cs.count = item['count']
            coded_symbols.append(cs)
        return coded_symbols

# 辅助函数，用于创建上下文值
def create_context_value(doc_id, version, content):
    return {"doc_id": doc_id, "version": version, "content": content}

class IBLTManager:
    def __init__(self, context_dict, iblt_size=100, hash_function_count=3):
        """
        初始化IBLTManager。
        :param context_dict: 当前的上下文，一个键值对字典。
        :param iblt_size: IBLT的大小。
        :param hash_function_count: IBLT使用的哈希函数数量。
        """
        self.context = context_dict
        self.iblt_size = iblt_size
        self.hash_function_count = hash_function_count
        self.iblt = self._create_iblt_from_context()

    def _create_iblt_from_context(self):
        """从当前上下文字典创建一个IBLT。"""
        iblt = IBLT(self.iblt_size, self.hash_function_count)
        for key, value in self.context.items():
            # IBLT要求key和value都是bytes
            key_bytes = str(key).encode('utf-8')
            value_bytes = json.dumps(value).encode('utf-8')
            iblt.insert(key_bytes, value_bytes)
        return iblt

    def encode_context(self):
        """将当前上下文编码为序列化的IBLT。"""
        return self.iblt.serialize()

    @staticmethod
    def decode_difference(local_context, authoritative_iblt_bytes, iblt_size=100, hash_function_count=3):
        """
        比较本地上下文和权威IBLT，解码出差异。
        :param local_context: 本地智能体的上下文字典。
        :param authoritative_iblt_bytes: 从meta端接收到的序列化IBLT。
        :param iblt_size: IBLT的大小（必须与meta端一致）。
        :param hash_function_count: 哈希函数数量（必须与meta端一致）。
        :return: 一个包含新增、更新和删除的键的元组 (added_keys, updated_keys, removed_keys)。
        """
        # 1. 创建本地IBLT
        local_iblt_manager = IBLTManager(local_context, iblt_size, hash_function_count)
        local_iblt = local_iblt_manager.iblt

        # 2. 加载权威IBLT
        authoritative_iblt = IBLT.from_serialized(authoritative_iblt_bytes, iblt_size, hash_function_count)

        # 3. 计算差异 (XOR操作)
        diff_iblt = local_iblt ^ authoritative_iblt

        # 4. 解码差异
        added_or_updated_items, removed_items = diff_iblt.list_entries()

        added_keys = []
        updated_keys = []
        removed_keys = [key.decode('utf-8') for key, _ in removed_items]

        for key_bytes, value_bytes in added_or_updated_items:
            key = key_bytes.decode('utf-8')
            if key in local_context:
                # 如果key存在于本地，说明是更新
                updated_keys.append(key)
            else:
                # 否则是新增
                added_keys.append(key)

        return added_keys, updated_keys, removed_keys