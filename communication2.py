import asyncio
import json
import time
from nats.aio.client import Client as NATS
from nats.js.api import StreamConfig
import re
import logging
from consistent_hash import ConsistentHashing

META_REGISTER_CHANNEL = "meta.register"

# 获取子任务结果频道
def get_task_result_channel(task_id):
    return f"{task_id}.result"

# 监听子智能体注册/注销，动态维护注册表和一致性哈希环
def agent_registry_listener(agent_registry, capability_rings, js):
    async def message_handler(msg):
        try:
            data = json.loads(msg.data.decode())
            msg_type = data["header"]["type"]
            payload = data["payload"]
            agent_id = payload["agent_id"]

            if msg_type == "register":
                capabilities = payload["capabilities"].split(",")
                listen_channel = payload["listen_channel"]
                status = payload["status"]

                # 更新注册表
                agent_registry[agent_id] = {
                    "capabilities": capabilities,
                    "listen_channel": listen_channel,
                    "status": status
                }
                for cap in capabilities:
                    cap = cap.strip()
                    if cap not in capability_rings:
                        capability_rings[cap] = ConsistentHashing()
                    # 根据agent能力数量调整虚拟节点数，能力越多，虚拟节点越少
                    vnodes = max(1, 10 - len(capabilities))
                    capability_rings[cap].add_node(agent_id, replicas=vnodes)
                print(f"[注册] : {capability_rings[cap]}")  
                print(f"[注册表] 新增/更新: {agent_id} 能力: {capabilities}")
                # 更新一致性哈希环

                logging.info(f"[注册] 新增/更新: {agent_id} 能力: {capabilities}")

            elif msg_type == "unregister":
                if agent_id in agent_registry:
                    # 从所有相关的哈希环中移除节点
                    for cap in agent_registry[agent_id]["capabilities"]:
                        if cap in capability_rings:
                            capability_rings[cap].remove_node(agent_id)
                    del agent_registry[agent_id]
                print(f"[注册表] 注销: {agent_id}")
                logging.info(f"[注册] 注销： {agent_id}")

        except Exception as e:
            print(f"[注册表] 处理消息异常: {e}")
            logging.info(f"[注册] 处理消息异常: {e}")
        await msg.ack()
    return message_handler

# 监听子任务结果
def result_listener(result_dict, js, task_ids, TASKS, agent_registry, busy_agent_sketch):
    async def message_handler(msg):
        try:
            data = json.loads(msg.data.decode())
            header = data.get("header", {})
            payload = data.get("payload", {})
            if header.get("type") == "subtask-re":
                task_id = None
                subject = msg.subject
                m = re.match(r"TASK_(\d+)_RESULT", subject)
                if m:
                    task_id = int(m.group(1))
                else:
                    task_id = payload.get("task_id")
                agent_id = payload.get("agent_id")
                result = payload.get("result")
                # 找到对应task
                task = next((t for t in TASKS if t["id"] == task_id), None)
                if task is not None:
                    if isinstance(result, list):
                        result = "\n".join(str(x) for x in result)
                    else:
                        result = str(result)
                    task["results"].append(result)
                    # task["current_stage"] += 1 # 移动到main.py中处理
                    print(f"[结果] 任务{task_id} 阶段{task['current_stage']} 结果: {result}")
                    logging.info(f"[结果] 任务{task_id} 阶段{task['current_stage']} 结果: {result}")
                    # 复位agent
                    if agent_id and busy_agent_sketch.contains(agent_id):
                        busy_agent_sketch.delete(agent_id)
                        print(f"[状态] agent {agent_id} 置为idle")
                        logging.info(f"[状态] agent {agent_id} 置为idle")
                    # 判断是否完成
                    if task["current_stage"] >= len(task["subtasks"]):
                        task["finished"] = True
                        print(f"[主控] 任务{task_id}已完成，结果: {task['results']}")
                        logging.info(f"[主控] 任务{task_id}已完成，结果: {task['results']}")
            await msg.ack()
        except Exception as e:
            print(f"[结果监听] 处理消息异常: {e}")
            logging.error(f"[结果监听] 处理消息异常: {e}")
    return message_handler

# 发布任务到指定子智能体频道
async def publish_subtask(js, listen_channel, task_id, query, iblt_data=None):
    msg = {
        "header": {
            "type": "subtask",
            "time": time.time()
        },
        "payload":{
            "task_id": task_id,
            "query": query,
            "iblt_data": iblt_data.hex() if iblt_data else None # 将bytes转为hex字符串以便JSON序列化
        }
    }
    await js.publish(listen_channel, json.dumps(msg).encode())
    print(f"[分发] 已发布任务{task_id}到{listen_channel}")
