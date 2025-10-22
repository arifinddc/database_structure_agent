# AI-Dope: The Schema Architect 🤖

> Hello! I am **AI-Dope**, your Database Schema Architect. Give me a description of your data needs, and I will design a tested, optimal, and anti-lag DDL blueprint.

---

## 1. Target Audience

This project is designed to facilitate the workflow of data and development professionals:

* **Data Engineers** looking for quick inspiration or verification for complex DDL designs.
* **Backend/Full-Stack Developers** who require an already optimized database schema based on specific use cases (OLTP, OLAP, HTAP, etc.).
* **Database Administrators (DBA)** who want to simulate query performance and compare different data processing types.

---

## 2. Core Functionality (How the Chatbot Works)

The chatbot functions as a **"Co-Architect AI"** to enhance the schema design process:

* **DDL Generation and Validation:** Translates narrative requirements into complete and correct DDL code (SQL `CREATE TABLE`).
* **Optimization Recommendations:** Intelligently recommends the most suitable data processing category (such as HTAP, OLTP, OLAP) and optimizes the DDL with appropriate indexing and partitioning.
* **Performance Simulation:** Generates a detailed Performance Simulation Report based on anticipated row volume, comparing transaction latency versus analysis throughput.
* **DML Simulation:** Provides relevant DML query examples (`SELECT`), complete with a copyable Markdown table simulation of the output.

---

## 3. The Role of the AI Agent

The AI operates as an **Expert Database Architect Agent** with a structured role:

* The agent is tasked with translating business concepts into structured **DDL architectural specifications**.
* The agent orchestrates a multi-step workflow using **LangGraph (ReAct)**, ensuring every design passes through the stages of Recommendation, Initial DDL, Optimization, and Simulation.
* It ensures the DDL output is correctly ordered based on `FOREIGN KEY` dependencies (Topological Sort) for correct execution order.
* It utilizes internal Tools defined in `database_tools.py` such as `estimate_query_performance` and `check_and_optimize_schema`.

---

## 4. Technology Stack (AI Model & Frameworks)

The application is built on a modern AI stack focused on reliability and structured output:

* **Primary AI Model:** **Gemini 2.5 Flash**, chosen for its speed and adherence to complex Agentic instructions.
* **Integration:** The model is integrated via the **LangChain Google Generative AI** library (`ChatGoogleGenerativeAI`).
* **Orchestration:** The agent logic is managed using the **LangGraph** framework (`create_react_agent`), ensuring the Agent follows the strict design workflow.
* **Frontend:** **Streamlit** is used for the interactive web interface.
* **Key Dependencies:** `streamlit`, `langchain-google-genai`, `langgraph`, `pandas`.

***

## 🚀 Installation and Local Setup

Follow these steps to run the project on your local machine:

1.  **Clone the Repository:**
    ```bash
    git clone [https://github.com/arifinddc/database_structure_agent](https://github.com/arifinddc/database_structure_agent) # Replace with your repo link
    cd database_structure_agent
    ```
2.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
3.  **Setup API Key:** Create a folder named `.streamlit` in the project root and add a file named `secrets.toml` inside it:
    ```toml
    # .streamlit/secrets.toml
    google_api_key="YOUR_GEMINI_API_KEY_HERE"
    ```
4.  **Run the Application:**
    ```bash
    streamlit run schema_designer_app.py
    ```