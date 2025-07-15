import re
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime

# 1. 解析metaagent.log
logfile = "metaagent.log"
assign_pattern = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+) INFO \[分发\] 任务(\d+) 阶段(\d+) 分配给(\w+)，内容: (.+)')
idle_pattern = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d+) INFO \[状态\] agent (\w+) 置为idle')

assignments = []
finishes = []

with open(logfile, encoding='utf-8') as f:
    for line in f:
        m = assign_pattern.search(line)
        if m:
            t, task_id, stage, agent_id, content = m.groups()
            t = datetime.strptime(t, "%Y-%m-%d %H:%M:%S,%f")
            assignments.append({
                "agent_id": agent_id,
                "task_id": int(task_id),
                "stage": int(stage),
                "content": content.strip(),
                "start": t
            })
        m2 = idle_pattern.search(line)
        if m2:
            t, agent_id = m2.groups()
            t = datetime.strptime(t, "%Y-%m-%d %H:%M:%S,%f")
            finishes.append({
                "agent_id": agent_id,
                "end": t
            })

# 2. 合并分发和idle，得到每个agent每个任务的执行区间
df_assign = pd.DataFrame(assignments)
df_finish = pd.DataFrame(finishes)

# 按分发顺序为每个agent分配end时间
df_assign['end'] = None
for agent in df_assign['agent_id'].unique():
    agent_assigns = df_assign[df_assign['agent_id'] == agent].sort_values('start')
    agent_finishes = df_finish[df_finish['agent_id'] == agent].sort_values('end')
    for i, (idx, row) in enumerate(agent_assigns.iterrows()):
        if i < len(agent_finishes):
            df_assign.loc[idx, 'end'] = agent_finishes.iloc[i]['end']

# 3. 画Gantt风格柱状图
agents = sorted(df_assign['agent_id'].unique())
fig, ax = plt.subplots(figsize=(18, 8))
colors = plt.cm.get_cmap('tab20', len(agents))

for i, agent in enumerate(agents):
    agent_tasks = df_assign[df_assign['agent_id'] == agent]
    for _, row in agent_tasks.iterrows():
        if pd.isnull(row['end']):
            continue
        ax.barh(
            y=i,
            width=(row['end'] - row['start']).total_seconds(),
            left=(row['start'] - df_assign['start'].min()).total_seconds(),
            height=0.6,
            color=colors(i),
            edgecolor='black'
        )
        # 标注任务内容
        ax.text(
            (row['start'] - df_assign['start'].min()).total_seconds() + 1,
            i,
            f"{row['task_id']}-{row['stage']}",
            va='center', ha='left', fontsize=5, color='black'
        )

ax.set_yticks(range(len(agents)))
ax.set_yticklabels(agents)
ax.set_xlabel("time(second,start at the first task assign)")
ax.set_title("13 sub agents Gantt")
plt.tight_layout()
plt.show()