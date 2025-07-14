import agentscope
from agentscope.agents import ReActAgentV2
from agentscope.message import Msg
from agentscope.service import ServiceToolkit
    

def RoutingAgent(api_key=None):
    agentscope.init(
        model_configs=[
        {
            "model_type": "openai_chat",
            "config_name": "openAI",
            "model_name": "gpt-4o-2024-08-06",
            "api_key": api_key  ,# API 密钥
            "client_args": {"base_url":"https://api.sttai.cc/v1" , },
            "generate_args": {"temperature": 0,},
        },],
    )
    ReAct_Agent = ReActAgentV2(
        name="meta",
        model_config_name="openAI",
        service_toolkit=ServiceToolkit(),
        sys_prompt="You are going to compose and decompose tasks.",
        max_iters=20,
    )
    return ReAct_Agent


def Routing(query,agent):
    task=Msg(
        role="user",
        content=query,
        name="user",
    )
    response=agent(task)
    return response.content
