import asyncio
import json
import time
from nats.aio.client import Client as NATS
from nats.js.api import StreamConfig

META_REGISTER_CHANNEL = "meta.register"

# 获取子任务结果频道
def get_task_result_channel(task_id):
    return f"{task_id}.result"

# 监听子智能体注册/注销，动态维护注册表
def agent_registry_listener(agent_registry, capability_queues, js):
    async def message_handler(msg):
        try:
            data = json.loads(msg.data.decode())
            msg_type = data["header"]["type"]
            if msg_type == "register":
                payload = data["payload"]
                agent_id = payload["agent_id"]
                capabilities = payload["capabilities"]
                listen_channel = payload["listen_channel"]
                status = payload["status"]
                # 先将agent_id从所有能力队列中移除
                for cap, q in capability_queues.items():
                    if agent_id in q:
                        q.remove(agent_id)
                # 再根据新能力分配
                agent_registry[agent_id] = {
                    "capabilities": capabilities.split(","),
                    "listen_channel": listen_channel,
                    "status": status
                }
                for cap in capabilities.split(","):
                    cap = cap.strip()
                    if cap not in capability_queues:
                        capability_queues[cap] = []
                    if agent_id not in capability_queues[cap]:
                        capability_queues[cap].append(agent_id)
                print(f"[注册表] 新增/更新: {agent_id} 能力: {capabilities}")
            elif msg_type == "unregister":
                payload = data["payload"]
                agent_id = payload["agent_id"]
                if agent_id in agent_registry:
                    del agent_registry[agent_id]
                for cap, q in capability_queues.items():
                    if agent_id in q:
                        q.remove(agent_id)
                print(f"[注册表] 注销: {agent_id}")
        except Exception as e:
            print(f"[注册表] 处理消息异常: {e}")
        await msg.ack()
    return message_handler

# 监听子任务结果
def result_listener(result_dict, js, task_ids):
    async def message_handler(msg):
        try:
            data = json.loads(msg.data.decode())
            header = data.get("header", {})
            if header.get("type") == "subtask-re":
                for task_id in task_ids:
                    if get_task_result_channel(task_id) == msg.subject:
                        result_dict[task_id] = data["payload"]["result"]
                        print(f"[结果] 收到{task_id}结果: {data['payload']['result']}")
        except Exception as e:
            print(f"[结果] 处理消息异常: {e}")
        await msg.ack()
    return message_handler

# 发布子任务到指定子智能体频道
async def publish_subtask(js, listen_channel, task_id, query):
    msg = {
        "header": {
            "type": "subtask",
            "time": time.time()
        },
        "payload":{
            "task_id": task_id,
            "query": query  
        }
    }
    await js.publish(listen_channel, json.dumps(msg).encode())
    print(f"[分发] 已发布子任务{task_id}到{listen_channel}")
