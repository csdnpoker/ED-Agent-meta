import hashlib
import bisect

class ConsistentHashing:
    def __init__(self, nodes=None, replicas=10):
        """
        :param nodes:         初始节点
        :param replicas:      每个节点对应的虚拟节点数
        """
        self.replicas = replicas
        self.ring = dict()
        self.sorted_keys = []
        if nodes:
            for node in nodes:
                self.add_node(node)

    def add_node(self, node, replicas=None):
        """
        添加node，同时根据replicas增加虚拟节点
        """
        replicas = replicas if replicas is not None else self.replicas
        for i in range(replicas):
            virtual_node_key = f"{node}-{i}"
            key = self._hash(virtual_node_key)
            self.ring[key] = node
            bisect.insort(self.sorted_keys, key)

    def remove_node(self, node):
        """
        移除node，同时移除对应的虚拟节点
        """
        # 确定要移除的虚拟节点数量
        # 由于我们无法直接知道添加时用了多少replicas，这里假设一个默认值或从外部传入
        # 在实际应用中，最好在添加节点时记录其replicas数量
        replicas_to_remove = self.replicas # 这是一个简化的假设
        keys_to_remove = []
        for i in range(replicas_to_remove):
            virtual_node_key = f"{node}-{i}"
            key = self._hash(virtual_node_key)
            if key in self.ring and self.ring[key] == node:
                keys_to_remove.append(key)
        
        for key in keys_to_remove:
            del self.ring[key]
            # 使用bisect.bisect_left和del来高效移除元素
            idx = bisect.bisect_left(self.sorted_keys, key)
            if idx < len(self.sorted_keys) and self.sorted_keys[idx] == key:
                del self.sorted_keys[idx]

    def get_node(self, key_str):
        """
        顺时针查找离key_str最近的节点
        """
        if not self.ring:
            return None
        key = self._hash(key_str)
        # 查找第一个大于等于key的虚拟节点的索引
        idx = bisect.bisect_left(self.sorted_keys, key)
        # 如果索引等于列表长度，说明key大于所有虚拟节点，回到环的起点
        if idx == len(self.sorted_keys):
            idx = 0
        return self.ring[self.sorted_keys[idx]]

    def _hash(self, key_str):
        """
        计算哈希值
        """
        m = hashlib.md5()
        m.update(str(key_str).encode('utf-8'))
        return int(m.hexdigest(), 16)