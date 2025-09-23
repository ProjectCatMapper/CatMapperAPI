# rag_neo4j.py

from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer
import chromadb
import openai
from openai import OpenAI
import os
from CM.utils import getDriver


# ------------------- CONFIG -------------------

# Neo4j Config

# OpenAI API key
openai.api_key = api_key

# Chroma settings
CHROMA_PERSIST_DIR = "./chroma"

# ------------------- STEP 1: Extract Data from Neo4j -------------------



# def get_graph_data():
#     driver = GraphDatabase.driver('neo4j://sociomap.rc.asu.edu:7687', auth=(
#                 'u', 'p'))
#     query = """
#     MATCH (n)-[r]->(m)
#     RETURN n, r, m
#     LIMIT 1000
#     """
#     with driver.session() as session:
#         results = session.run(query)
#         records = []
#         for record in results:
#             n = dict(record["n"])
#             r = record["r"].type
#             m = dict(record["m"])
#             n_label = list(record["n"].labels)[0] if record["n"].labels else "Node"
#             m_label = list(record["m"].labels)[0] if record["m"].labels else "Node"
#             sentence = f"{n_label} {n} -[{r}]-> {m_label} {m}"
#             records.append(sentence)
#         return records

# # ------------------- STEP 2: Embed and Store in Vector DB -------------------

# def embed_and_store(texts):
#     # Initialize embedding model
#     embedder = SentenceTransformer("all-MiniLM-L6-v2")
#     embeddings = embedder.encode(texts)

#     # Initialize Chroma
#     chroma_client = chromadb.PersistentClient(path=CHROMA_PERSIST_DIR)
#     collection = chroma_client.get_or_create_collection(name="neo4j_rag")

#     for i, (text, emb) in enumerate(zip(texts, embeddings)):
#         collection.add(documents=[text], embeddings=[emb.tolist()], ids=[str(i)])
    
#     return embedder, collection

# # ------------------- STEP 3: Retrieve Context -------------------

# def retrieve_context(query, embedder, collection, top_k=3):
#     query_emb = embedder.encode([query])[0]
#     results = collection.query(query_embeddings=[query_emb.tolist()], n_results=top_k)
#     return results["documents"][0]

# # ------------------- STEP 4: Generate Answer -------------------

# def generate_answer(query, context):
#     prompt = f"""
# You are a helpful assistant using the following Neo4j graph knowledge:

# {context}

# User question: {query}
# Answer:
# """
#     response = openai.ChatCompletion.create(
#         model="gpt-4",
#         messages=[{"role": "user", "content": prompt}]
#     )
#     return response['choices'][0]['message']['content']

# # ------------------- MAIN -------------------

# if __name__ == "__main__":
#     print("📦 Extracting data from Neo4j...")
#     texts = get_graph_data()

#     print("🔍 Embedding and storing...")
#     embedder, collection = embed_and_store(texts)

#     while True:
#         query = input("\nAsk a question about the graph (or 'exit'): ")
#         if query.lower() in ["exit", "quit"]:
#             break
#         print("💬 Retrieving context...")
#         context = retrieve_context(query, embedder, collection)
#         print("🧠 Generating answer...")
#         answer = generate_answer(query, context)
#         print(f"\n✅ Answer:\n{answer}")


# Load Chroma collection
client = OpenAI(api_key= api_key)
chroma_client = chromadb.PersistentClient(path=CHROMA_PERSIST_DIR)
collection = chroma_client.get_collection(name="neo4j_rag")

# Load same embedder as before
embedder = SentenceTransformer("all-MiniLM-L6-v2")

# Function to get relevant context
def retrieve_context(query, top_k=3,max_chars=1500):
    query_emb = embedder.encode([query])[0]
    results = collection.query(query_embeddings=[query_emb.tolist()], n_results=top_k)
    docs = results["documents"][0]
    truncated_docs = [doc[:max_chars] for doc in docs]
    return "\n\n".join(truncated_docs)

# Generate answer with OpenAI
def generate_answer(query, context):
    prompt = f"""
You are a helpful assistant using the following Neo4j graph knowledge:

{context}

User question: {query}
Answer:
"""
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )
    return response.choices[0].message.content

# --- RAG loop ---
if __name__ == "__main__":
    while True:
        query = input("\nAsk a question (or 'exit'): ")
        if query.lower() in ["exit", "quit"]:
            break
        context = retrieve_context(query)
        answer = generate_answer(query, context)
        print(f"\n✅ Answer:\n{answer}")