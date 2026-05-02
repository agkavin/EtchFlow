from typing import TypedDict
import time
from langgraph.graph import StateGraph, END
from etchflow import EtchFlow

# 1. Define the State (The shared clipboard for our nodes)
class AgentState(TypedDict):
    topic: str
    plan: str
    research: str
    content: str
    formatted_blog: str

# 2. Define the Nodes (The Functions)
# We add sleep to simulate real work and give time for the kill-test
def plan_step(state: AgentState):
    print("Step 1: Planning...", flush=True)
    time.sleep(2)
    return {"plan": f"A 3-part structure for {state['topic']}"}

def research_step(state: AgentState):
    print("Step 2: Researching...", flush=True)
    time.sleep(2)
    return {"research": f"Detailed notes on {state['topic']} sourced from 'The Internet'"}

def write_step(state: AgentState):
    print("Step 3: Writing...", flush=True)
    time.sleep(2)
    draft = f"Draft based on: {state['plan']}. \nData: {state['research']}"
    return {"content": draft}

def format_step(state: AgentState):
    print("Step 4: Formatting...", flush=True)
    time.sleep(2)
    # Adding some simple Markdown-style formatting
    final = f"# {state['topic']}\n\n{state['content']}\n\n---\n*Written by AI*"
    return {"formatted_blog": final}

def cleanup_step(state: AgentState):
    print("Step 5: Final Cleanup...", flush=True)
    time.sleep(2)
    # Simulating a final polish or metadata addition
    return {"formatted_blog": state["formatted_blog"] + "\n[Ready to Publish]"}

# 3. Connect the Nodes into a Graph
workflow = StateGraph(AgentState)

workflow.add_node("planner", plan_step)
workflow.add_node("researcher", research_step)
workflow.add_node("writer", write_step)
workflow.add_node("formatter", format_step)
workflow.add_node("cleanup", cleanup_step)

# Define the straight-line flow
workflow.set_entry_point("planner")
workflow.add_edge("planner", "researcher")
workflow.add_edge("researcher", "writer")
workflow.add_edge("writer", "formatter")
workflow.add_edge("formatter", "cleanup")
workflow.add_edge("cleanup", END)

# 4. Initialize EtchFlow and compile
etchflow = EtchFlow("http://localhost:8080")
app = etchflow.compile(workflow)

# 5. Run the workflow
def main():
    import sys
    
    # A thread_id is REQUIRED (this matches standard LangGraph checkpointers).
    # Because we updated the backend to support TEXT, you can use any string!
    # If this ID is new, it starts fresh. If it crashed midway, it resumes. If finished, it returns the final state.
    run_id = "demo-run-123"
    if "--run-id" in sys.argv:
        idx = sys.argv.index("--run-id")
        run_id = sys.argv[idx + 1]
        
    config = {"configurable": {"thread_id": run_id}}
    
    try:
        final_state = app.invoke({"topic": "The History of Coffee"}, config=config)
        print("\n--- FINAL OUTPUT ---\n")
        print(final_state.get("formatted_blog", "N/A"))
        print("\nRun complete ✅")
    except StopIteration as e:
        print(f"\nRun complete ✅ ({e})")

if __name__ == "__main__":
    main()
