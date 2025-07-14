import asyncio
import os
import dotenv
import time
import json
import re
from nats.aio.client import Client as NATS
from nats.js.api import StreamConfig
from communication import agent_registry_listener, result_listener, publish_subtask, get_task_result_channel
from agent import RoutingAgent, Routing
import logging

parent_dir = os.path.dirname(os.path.abspath(__file__))
dotenv.load_dotenv(os.path.join(parent_dir, ".env"))
IP = os.getenv("IP")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# 任务队列示例
RAW_TASKS = [
    {"id": 1, "content": "写一篇关于人工智能发展史的短文，并用英文总结其未来趋势。"},
    {"id": 2, "content": "计算2024年中国GDP增长率，并用中文解释其影响。"},
    {"id": 3, "content": "润色以下英文段落并总结其主要观点：Artificial intelligence is transforming industries worldwide."},
    {"id": 4, "content": "分析气候变化对农业的影响，生成一段分析报告并润色。"},
    {"id": 5, "content": "生成一段关于区块链技术的介绍，推理其在金融领域的应用，并总结。"},
    {"id": 6, "content": "计算斐波那契数列第20项，并用英文解释其计算过程。"},
    {"id": 7, "content": "对以下技术文档进行语法润色和分析总结：The system architecture consists of multiple agents."},
    {"id": 8, "content": "生成一段关于未来交通的畅想，润色后分析其可行性并总结。"},
    {"id": 9, "content": "计算2025/1/1距离今天多少天，并用英文解释其意义。"},
    {"id": 10, "content": "写一段关于健康生活的建议，润色并总结其核心要点。"},
    {"id": 11, "content": "生成一段关于机器学习的介绍，推理其在医疗领域的潜力，并用中文总结。"},
    {"id": 12, "content": "对以下段落进行语法润色和分析：Data privacy is a major concern in cloud computing."},
    {"id": 13, "content": "写一篇关于新能源车市场的分析报告，并用英文总结其发展趋势。"},
    {"id": 14, "content": "计算圆周率到小数点后10位，并用中文解释其计算方法。"},
    {"id": 15, "content": "生成一段关于5G技术的介绍，分析其对社会的影响并总结。"},
    {"id": 16, "content": "对以下英文句子进行语法润色并总结其含义：The quick brown fox jumps over the lazy dog."},
    {"id": 17, "content": "写一段关于可持续发展的建议，推理其对环境的益处并总结。"},
    {"id": 18, "content": "分析人工智能在教育领域的应用，生成一段分析并润色。"},
    {"id": 19, "content": "计算100以内所有质数的和，并用英文解释其意义。"},
    {"id": 20, "content": "生成一段关于数字货币的介绍，分析其风险并用中文总结。"},
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

async def main():
    # 初始化NATS/JetStream
    nc = NATS()
    await nc.connect(IP)
    js = nc.jetstream()
    try:
        await js.add_stream(name="META_REGISTER", subjects=["meta.register"])
    except Exception:
        pass
    agent_registry = {}
    capability_queues = {}
    reg_sub = await js.subscribe("meta.register", cb=agent_registry_listener(agent_registry, capability_queues, js), durable="META_REG_DURABLE")
    # 初始化agent
    agent = RoutingAgent(OPENAI_API_KEY)
    # 拆解所有任务
    TASKS = []
    logging.basicConfig(filename='metaagent.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    for raw_task in RAW_TASKS:
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
        ch = f"TASK_{task['id']}_RESULT"  # 保证与子智能体一致
        try:
            await js.add_stream(name=f"TASK_{task['id']}_RESULT", subjects=[ch])
        except Exception:
            pass
        sub = await js.subscribe(ch, cb=result_listener(result_dict, js, [task["id"]]), durable=f"TASK_{task['id']}_DURABLE")
        result_subs.append(sub)
    print("[主控] 启动主循环...")
    logging.info("[主控] 启动主循环...")
    while not all([t["finished"] for t in TASKS]):
        await asyncio.sleep(0.2)
        for task in TASKS:
            if task["finished"]:
                continue
            stage = task["current_stage"]
            if stage >= len(task["subtasks"]):
                task["finished"] = True
                print(f"[主控] 任务{task['id']}已完成，结果: {task['results']}")
                logging.info(f"[主控] 任务{task['id']}已完成，结果: {task['results']}")
                continue
            subtask = task["subtasks"][stage]
            required_cap = subtask["ability"]
            agent_id = None
            for aid in capability_queues.get(required_cap, []):
                agent_info = agent_registry.get(aid, {})
                if agent_info.get("status") == "idle":
                    agent_id = aid
                    break
            if agent_id:
                agent_registry[agent_id]["status"] = "busy"
                listen_channel = agent_registry[agent_id]["listen_channel"]
                overall_task = RAW_TASKS[task["id"]-1]["content"] if task["id"]-1 < len(RAW_TASKS) else ""
                dependency_results = "" if stage == 0 else "\n".join(task["results"])
                additional_info = "None"
                query = f"""
We are solving a complex task, and we have split the task into several subtasks.
You need to process one given task. Don’t assume that the problem is
unsolvable. The answer does exist. If you can’t solve the task, please
describe the reason and the result you have achieved in detail.
The content of the task that you need to do is:
<task>
{subtask['task']}
</task>
Here is the overall task for reference, which contains some helpful
information that can help you solve the task:
<overall_task>
{overall_task}
</overall_task>
Here are results of some prerequisite results that you can refer to (empty if
there are no prerequisite results):
<dependency_results_info>
{dependency_results}
</dependency_results_info>
Here are some additional information about the task (only for reference, and
may be empty):
<additional_info>
{additional_info}
</additional_info>
Now please fully leverage the information above, try your best to leverage
the existing results and your available tools to solve the current task.
"""
                print(f"[分发] 任务{task['id']} 阶段{stage} 分配给{agent_id}，内容: {subtask['task']}")
                logging.info(f"[分发] 任务{task['id']} 阶段{stage} 分配给{agent_id}，内容: {subtask['task']}")
                await publish_subtask(js, listen_channel, task["id"], query)
            if task["id"] in result_dict:
                result = result_dict.pop(task["id"])
                task["results"].append(result)
                print(f"[结果] 任务{task['id']} 阶段{stage} 结果: {result}")
                logging.info(f"[结果] 任务{task['id']} 阶段{stage} 结果: {result}")
                task["current_stage"] += 1
                if agent_id:
                    agent_registry[agent_id]["status"] = "idle"
                    print(f"[状态] agent {agent_id} 置为idle")
                    logging.info(f"[状态] agent {agent_id} 置为idle")
    print("[主控] 所有任务已完成！")
    logging.info("[主控] 所有任务已完成！")
    # 所有任务完成后，通知所有子智能体 shutdown
    shutdown_msg = {
        "header": {
            "type": "shutdown",
            "time": time.time()
        }
    }
    for agent_id, info in agent_registry.items():
        listen_channel = info["listen_channel"]
        await js.publish(listen_channel, json.dumps(shutdown_msg).encode())
        print(f"[主控] 已向 {agent_id} ({listen_channel}) 发送 shutdown")
        logging.info(f"[主控] 已向 {agent_id} ({listen_channel}) 发送 shutdown")
    await nc.close()

if __name__ == "__main__":
    asyncio.run(main())
