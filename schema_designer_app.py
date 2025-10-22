# schema_designer_app.py - FINAL STREAMLIT AGENT APP (FIXED create_react_agent error and DML Simulation)

import streamlit as st
from langchain_google_genai import ChatGoogleGenerativeAI
from langgraph.prebuilt import create_react_agent
from langchain_core.messages import HumanMessage, AIMessage
from langchain_core.tools import tool
import re
import json
import sys # <-- NEW: Import sys for recursion limit

# LangGraph/LangChain uses the Python recursion limit. Setting it higher for safety.
# This replaces the unsupported 'config' parameter in create_react_agent.
try:
    sys.setrecursionlimit(2000) # Set to a safe high number
except RecursionError:
    pass

# Import all database tools
from database_tools import optimize_ddl, validate_schema, order_sql_commands, simulate_performance, simulate_dml_output 

# --- 0. API Key Handling from st.secrets ---

APP_TITLE_PART_2 = "Database Structure & DDL Agent 💾"
google_api_key = None 

try:
    # IMPORTANT: Ensure your .streamlit/secrets.toml file contains google_api_key = "YOUR_KEY"
    google_api_key = st.secrets["google_api_key"]
except KeyError:
    st.error("🚨 Google AI API Key ('google_api_key') not found in st.secrets. Please ensure your `.streamlit/secrets.toml` file is correctly configured.")
    st.stop() 
except Exception as e:
    st.error(f"🚨 An error occurred while reading st.secrets: {e}")
    st.stop()
    
if not google_api_key: 
    st.error("🚨 The API Key is empty. Please check your `.streamlit/secrets.toml` file.")
    st.stop()

# --- 1. Page Configuration and Title ---

st.title("🤖 AI-Dope: The Schema Architect")
st.caption("The AI Agent delivering hyper-optimized, bug-proof DDL and data logic blueprints.")
st.markdown("---")

# --- Tool Definitions (The Actions of the Agent) ---

@tool
def check_and_optimize_schema(ddl_string: str, usage_type: str) -> str:
    """Analyze the generated DDL, suggest optimal indexing, and partitioning based on usage type (OLTP, OLAP, HTAP, Batch, Stream, or OLLP)."""
    return optimize_ddl(ddl_string, usage_type)

@tool
def perform_schema_quality_assurance(ddl_string: str, sample_data_json: str) -> str:
    """Validate the DDL against provided sample data (in JSON format)."""
    return validate_schema(ddl_string, sample_data_json)

@tool
def estimate_query_performance(ddl_string: str, row_count: int, proposed_usage_type: str) -> str:
    """
    Simulate query time estimation for the given DDL and data volume (row_count).
    Compares performance across different usage types (OLTP, OLAP, HTAP, etc.) 
    to validate the best choice for the user.
    """
    return simulate_performance(ddl_string, row_count, proposed_usage_type) 

@tool
def simulate_dml_output(select_query: str, result_description: str) -> str:
    """
    Use this tool ONLY when providing example SELECT queries to the user. 
    This tool simulates the output of the SELECT query and formats it as a copyable Markdown table for the user.
    'result_description' must be a concise explanation of the data the query returns (e.g., 'Team A member list').
    """
    return simulate_dml_output(select_query, result_description)


# --- 2. Agent Initialization ---

if "agent" not in st.session_state:
    try:
        llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash",
            google_api_key=google_api_key,
            temperature=0.2
        )
        
        # --- ENGLISH HYBRID PROMPT (WITH CRITICAL STOP CONDITION) ---
        english_hybrid_prompt = """
        You are an **Expert Database Architect and Data Engineer** (Master Schema Designer). Your primary goal is to design optimal, reliable, and performant SQL schemas (DDL) and provide best practice DML kueries.

        **CONTEXT: DATA PROCESSING CATEGORIES:**
        1. **OLTP (Online Transaction Processing):** Fast, high-volume transactions.
        2. **OLAP (Online Analytical Processing):** Complex analysis of historical data.
        3. **HTAP (Hybrid Transactional/Analytical Processing):** Mixing OLTP and OLAP.
        4. **Batch Processing (Batch):** Processing large data volumes at scheduled times.
        5. **Stream Processing (Stream):** Analyzing data continuously.
        6. **Online Low-Latency Processing (OLLP):** Decisions in micro-seconds.
        
        **Agentic Workflow / Chain of Thought (MUST BE FOLLOWED)**:
        1. **DDL Generation**: Based on the user's request, **FIRST**, generate the complete and correct SQL DDL.
        
        2. **Consultation & Data Gathering (CRUCIAL STEP)**:
           a. Analyze the request and **explicitly state** which one of the 6 categories you recommend and **briefly explain why**.
           b. Present the initial DDL.
           c. **CRITICAL QUESTION**: **ASK** the user if they confirm the recommended type, and if they can provide the **ANTICIPATED TOTAL ROW COUNT (e.g., 10 million)** and **sample data** (JSON).
           
        3. **Performance Estimation (Tool Use)**: IF the row count is provided, MUST use the estimate_query_performance tool with the proposed type.
        ***IMMEDIATE OUTPUT ALERT***: After receiving the OBSERVED output from the performance tool, YOU MUST present the full Performance Simulation Report to the user as a partial answer before proceeding to the next optimization steps.

        4. **Schema Optimization (Tool Use)**: If the confirmed/chosen usage type is provided, **MUST** use the **check_and_optimize_schema** tool.
        
        5. **Quality Assurance (Tool Use)**: **IF** sample data is provided, **MUST** use the **perform_schema_quality_assurance** tool.
        
        6. **Final Output**: Present the final, optimized, and validated DDL to the user, using the performance estimation results to justify the final recommendation.
        
        **7. DML/Query Examples (CRITICAL ADDITION)**: 
        If the user asks for example SELECT queries, you **MUST** first generate the SQL, and then **MUST** use the **simulate_dml_output** tool immediately to show a simulated result table.
        ***CRITICAL STOP CONDITION***: Once you receive the output from the simulate_dml_output tool, you **MUST IMMEDIATELY** present the result to the user as the final output. **DO NOT** attempt to call any other tool or engage in further reasoning after this step. The tool output already contains the full formatted answer.
        
        **CRITICAL OUTPUT FORMATTING RULES (MUST BE ADHERED TO FOR ALL RESPONSES)**:
        * All SQL code MUST be presented in a **```sql...```** code block.
        * All JSON data MUST be presented in a **```json...```** code block.
        * **DDL COMMANDS (CREATE TABLE):** When presenting final DDL, it MUST be broken down **PER TABLE** and accompanied by an explanation. These commands will be internally ordered by FK dependencies.
        * **DML COMMANDS (SELECT, INSERT, UPDATE, DELETE):** When presenting DML commands (e.g., example SELECT queries or INSERT statements), they **MUST NOT** contain any internal DDL ordering comments. They should be presented clearly within the ```sql...``` block immediately after the textual explanation.
        """
        
        # --- FIXED AGENT INITIALIZATION: Removed the unsupported 'config' parameter ---
        st.session_state.agent = create_react_agent(
            model=llm,
            tools=[check_and_optimize_schema, perform_schema_quality_assurance, estimate_query_performance, simulate_dml_output], 
            prompt=english_hybrid_prompt
        )
    except Exception as e:
        st.error(f"Failed to initialize Agent: {e}")
        st.stop()


# --- 3. Chat History Management ---

if "messages" not in st.session_state:
    st.session_state.messages = []

if st.button("♻️ Reset Conversation", key="reset_chat"):
    st.session_state.pop("agent", None) 
    st.session_state.pop("messages", None)
    st.rerun()


# --- 4. Display Past Messages ---

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        if "ddl_code" in msg:
            code_blocks = msg["ddl_code"].split("\n\n---\n\n")
            for block in code_blocks:
                lang = 'sql' 
                if block.strip().startswith('{') or block.strip().startswith('['): 
                    lang = 'json'
                st.code(block, language=lang)
        
        st.markdown(msg["content"])


# --- 5. Handle User Input and Agent Communication ---

# FIX 1: Initialize 'answer' and other variables in the GLOBAL SCOPE.
# This prevents NameError during Streamlit's initial run (when prompt is empty).
answer = "" 
final_message_content = ""
stored_codes = [] 
code_block_pattern_full = r"```(sql|json|markdown)\n(.*?)```" 

prompt = st.chat_input("Describe the database schema you want to design...")


# THE ENTIRE EXECUTION, DISPLAY, AND STORAGE LOGIC MUST BE INSIDE THIS BLOCK
if prompt:
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    try:
        messages = []
        for msg in st.session_state.messages:
            if msg["role"] == "user":
                messages.append(HumanMessage(content=msg["content"]))
            elif msg["role"] == "assistant":
                messages.append(AIMessage(content=msg["content"]))

        with st.spinner("Processing with AI-Dope..."):
            response = st.session_state.agent.invoke({"messages": messages})
            
            final_message = response["messages"][-1]
            answer = str(final_message.content) # Defined here if successful

            # --- STAGE 1, 2, 3: (The long string cleanup logic) ---
            
            # Stage 1: Primary Extraction
            if isinstance(final_message.content, list):
                answer = " ".join([str(item) for item in final_message.content])
            elif hasattr(final_message, 'text') and final_message.text:
                answer = final_message.text
                
            # Stage 2: Metadata/Signature Cleanup (Removes garbage text)
            answer = re.sub(r"'extras':\s*\{\'signature\':\s*\'[^\']+\'\}", "", answer)
            answer = re.sub(r"\{\'type\':\s*\'text\',\s*\'text\':\s*\'?([^\}]+)\'?\}", r'\1', answer, flags=re.DOTALL)
            answer = re.sub(r'\\n', '\n', answer) 
            answer = answer.replace("}}", "").strip()

            # Stage 3: Performance Report Cleanup
            if "ESTIMATION FOR PROPOSED TYPE" in answer:
                estimation_match = re.search(r'(OLTP\|[^ ]+.*?min)\s*(STREAM\|[^ ]+.*?min)', answer, re.DOTALL)
                if estimation_match:
                    data_block = estimation_match.group(1) + " " + estimation_match.group(2)
                    parts = re.findall(r'([A-Z]+)\s*\|\s*([^|]+?)\s*\|\s*([^ ]+)', data_block)
                    if parts:
                        new_estimation_list = []
                        for proc_type, latency, throughput in parts:
                            new_estimation_list.append(f"- **{proc_type}**: Latency: {latency.strip()} / Throughput: {throughput.strip()}")
                        answer = answer.replace(estimation_match.group(0), "\n\n" + "\n".join(new_estimation_list) + "\n\n")

            final_message_content = answer.strip()

    except Exception as e:
        # FIX 2: Define 'answer' in the except block to ensure it's not undefined later
        error_message = f"Execution Error: An error occurred during agent execution: {e}. Please simplify your request."
        answer = error_message
        final_message_content = error_message
        
        with st.chat_message("assistant"):
            st.error(error_message)


    # --- DISPLAY LOGIC (Now safely inside the 'if prompt:' block) ---
    
    # Only proceed with display if the answer is not an internal error message
    if answer and not answer.startswith("Execution Error:"):
        
        stored_codes = [] 

        # 3. Display the assistant's response.
        with st.chat_message("assistant"):
            
            # Split the text (explanation) based on code blocks
            explanation_segments = re.split(code_block_pattern_full, answer, flags=re.DOTALL)
            
            i = 0
            while i < len(explanation_segments):
                
                # Display Explanation Text 
                segment = explanation_segments[i]
                if segment and segment.strip():
                    st.markdown(segment.strip())
                
                i += 1
                if i < len(explanation_segments):
                    lang_tag = explanation_segments[i].strip()
                    i += 1
                    if i < len(explanation_segments):
                        code_content_raw = explanation_segments[i]
                        code_content = code_content_raw.strip()
                        
                        if code_content:
                            final_display_content = code_content
                            if lang_tag.lower() == 'sql':
                                # This function must be imported at the top: from database_tools import order_sql_commands
                                final_display_content = order_sql_commands(code_content) 
                                
                            display_lang = 'markdown' if lang_tag.lower() == 'markdown' else lang_tag.lower()
                            st.code(final_display_content, language=display_lang)
                            stored_codes.append(code_content)
                    i += 1

        # 4. Add the assistant's response to the message history list (for memory)
        if final_message_content:
            msg_to_store = {"role": "assistant", "content": final_message_content}
            if stored_codes:
                msg_to_store["ddl_code"] = "\n\n---\n\n".join(stored_codes)
            st.session_state.messages.append(msg_to_store)