import asyncio
import os
import dotenv
import time
import json
import re
from nats.aio.client import Client as NATS
from nats.js.api import StreamConfig
from communication2 import agent_registry_listener, result_listener, publish_subtask, get_task_result_channel
from agent import RoutingAgent, Routing
import logging
from consistent_hash import ConsistentHashing
from cuckoopy import CuckooFilter
from iblt import RatelessIBLTManager # 导入 IBLT 相关模块

parent_dir = os.path.dirname(os.path.abspath(__file__))
dotenv.load_dotenv(os.path.join(parent_dir, ".env"))
IP = os.getenv("IP")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# 初始化Cuckoo Filter
busy_agent_sketch = CuckooFilter(capacity=1000, bucket_size=4, fingerprint_size=1)

# 初始化一致性哈希环
capability_rings = {}

# 任务队列示例
RAW_TASKS = [
    {"id": 1, "content": "写一篇关于人工智能发展史的短文，并用英文总结其未来趋势。"},
    {"id": 2, "content": "计算2024年中国GDP增长率，并用中文解释其影响。"},
    {"id": 3, "content": "润色以下英文段落并总结其主要观点：Artificial intelligence is transforming industries worldwide."},
    {"id": 4, "content": "分析气候变温影响农业，生成一段分析报告并润色。"},
    {"id": 5, "content": "生成一段关于区块链技术的介绍，推理其在金融领域的应用，并总结。"},
    {"id": 6, "content": "计算斐波那契数列第20项，并用英文解释其计算过程。"},
    {"id": 7, "content": "对以下技术文档进行语法润色和分析总结：The system architecture consists of multiple agents."},
    {"id": 8, "content": "生成一段关于未来交通的畅想，润色后分析其可行性并总结。"},
    {"id": 9, "content": "计算2025/1/1距离今天多少天，并用英文解释其意义。"},
    {"id": 10, "content": "写一段关于健康生活的建议，润色并总结其核心要点。"},
]

# 可用能力
ABILITIES = ["text generation", "mathematical reasoning", "grammar polish", "analysis and summary"]

# 构造任务拆解prompt
def build_split_prompt(task_content, abilities):
    return f'''
You need to split the given task into subtasks according to the workers available in
the group.
The content of the task is:
==============================
{task_content}
==============================
Following are the available workers, given in the format <ability>
==============================
{''.join([f'<{a}>' for a in abilities])}
==============================
You must return the subtasks in the format of a numbered list within <tasks> tags, as
shown below:
<tasks>
<task>Subtask 1</task><ability>one of text generation,grammar polish,mathematical reasoning and analysis and summary</ability>
<task>Subtask 2</task><ability>one of text generation,grammar polish,mathematical reasoning and analysis and summary</ability>
</tasks>
'''

# 解析<tasks>...</tasks>格式，返回[(子任务内容, 能力)]
def parse_tasks(xml_str):
    xml_str=str(xml_str)
    tasks = []
    m = re.search(r'<tasks>(.*?)</tasks>', xml_str, re.DOTALL)
    if not m:
        return []
    inner = m.group(1)
    task_items = re.findall(r'<task>(.*?)</task>\s*<ability>(.*?)</ability>', inner, re.DOTALL)
    for t, a in task_items:
        tasks.append({"task": t.strip(), "ability": a.strip()})
    return tasks

# 选择agent
def select_agent(capability, task_id, agent_registry):
    if capability not in capability_rings:
        print(f"[调度] 能力'{capability}'不存在哈希环")
        return None

    ring = capability_rings[capability]
    # 尝试最多len(agent_registry)次，避免死循环
    for _ in range(len(agent_registry)):
        agent_id = ring.get_node(str(task_id))
        if agent_id and not busy_agent_sketch.contains(agent_id):
            return agent_id
        # 如果agent忙碌，则尝试下一个节点
        task_id += 1
    return None

async def main():
    # 初始化NATS/JetStream
    nc = NATS()
    await nc.connect(IP)
    js = nc.jetstream()

    # 初始化 IBLT 管理器
    iblt_manager = RatelessIBLTManager()

    # 为每个任务创建模拟的权威上下文
    TASK_CONTEXTS = {}
    for i in range(1, 11):
        TASK_CONTEXTS[i] = {
            f"doc_{i}_1": f"This is the first document for task {i}.".encode('utf-8'),
            f"doc_{i}_2": f"This is the second document for task {i}.".encode('utf-8')
        }

    try:
        await js.add_stream(name="META_REGISTER", subjects=["meta.register"])
    except Exception:
        pass
    agent_registry = {}
    capability_queues = {}
    reg_sub = await js.subscribe("meta.register", cb=agent_registry_listener(agent_registry, capability_rings, js), durable="META_REG_DURABLE")
    
    # 拆解所有任务
    TASKS = []
    logging.basicConfig(filename='metaagent.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    for raw_task in RAW_TASKS:
        # 初始化agent
        agent = RoutingAgent(OPENAI_API_KEY)
        prompt = build_split_prompt(raw_task["content"], ABILITIES)
        split_result = Routing(prompt, agent)
        subtasks = parse_tasks(split_result)
        if not subtasks:
            print(f"[拆解] 任务{raw_task['id']}拆解失败，使用默认pipeline")
            logging.warning(f"[拆解] 任务{raw_task['id']}拆解失败，使用默认pipeline")
            subtasks = [{"task": raw_task["content"], "ability": "text generation"}]
        TASKS.append({"id": raw_task["id"], "subtasks": subtasks, "results": [], "current_stage": 0, "finished": False})
        print(f"[拆解] 任务{raw_task['id']}拆解结果: {subtasks}")
        logging.info(f"[拆解] 任务{raw_task['id']}拆解结果: {subtasks}")
    # 结果收集
    result_dict = {}
    result_subs = []
    for task in TASKS:
        ch = get_task_result_channel(task['id'])  # 保证与子智能体一致
        try:
            await js.add_stream(name=f"TASK_{task['id']}_RESULT", subjects=[ch])
        except Exception:
            pass
        sub = await js.subscribe(ch, cb=result_listener(result_dict, js, [task["id"]], TASKS, agent_registry, busy_agent_sketch), durable=f"TASK_{task['id']}_DURABLE")
        result_subs.append(sub)
    print("[主控] 启动主循环...")
    logging.info("[主控] 启动主循环...")
    while not all([t["finished"] for t in TASKS]):
        for task in TASKS:
            if not task["finished"] and task["current_stage"] < len(task["subtasks"]):
                subtask = task["subtasks"][task["current_stage"]]
                capability = subtask["ability"]
                agent_id = select_agent(capability, task["id"], agent_registry)
                if agent_id:
                    agent_info = agent_registry[agent_id]
                    # 更新agent状态为busy
                    busy_agent_sketch.insert(agent_id)

                    # --- IBLT 集成开始 ---
                    # 1. 获取当前任务的权威上下文
                    authoritative_context = TASK_CONTEXTS.get(task['id'], {})

                    # 2. 将上下文编码为 IBLT
                    iblt_serialized = iblt_manager.encode(authoritative_context)
                    # --- IBLT 集成结束 ---

                    print(f"[调度] 任务{task['id']}阶段{task['current_stage']}->{agent_id}({capability}) {agent_info['listen_channel']}")
                    logging.info(f"[调度] 任务{task['id']}阶段{task['current_stage']}->{agent_id}({capability}) {agent_info['listen_channel']}")
                    # 3. 发布任务，并附带 IBLT
                    await publish_subtask(js, agent_info["listen_channel"], task["id"], subtask["task"], iblt_serialized.encode('utf-8'))
                    task["current_stage"] += 1  # 假定立即发送成功
                else:
                    print(f"[调度] 任务{task['id']}阶段{task['current_stage']} 无可用agent({capability})")
                    logging.info(f"[调度] 任务{task['id']}阶段{task['current_stage']} 无可用agent({capability})")
        await asyncio.sleep(1)
    # 清理
    await reg_sub.unsubscribe()
    for sub in result_subs:
        await sub.unsubscribe()
    await nc.close()

if __name__ == '__main__':
    asyncio.run(main())
