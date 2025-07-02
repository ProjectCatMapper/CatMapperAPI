import os
import ast
import networkx as nx
import matplotlib.pyplot as plt
from collections import defaultdict
import pandas as pd
import fnmatch

EXCLUDED_FILES = {
    "upload.py",
    "semantic_search_fine_tuner.py",
    "test.py",
    "wsgi.py",
    "rag.py",
    "__init__.py"
}
EXCLUDED_DIRS = {'.git', '__pycache__', '.venv', 'env', 'build', 'dist', '.vscode','lib','lib64'}
def save_edgelist_to_excel(G, output_file="function_call_edgelist.xlsx"):
    edges = list(G.edges())
    df = pd.DataFrame(edges, columns=["Caller", "Callee"])
    df.to_excel(output_file, index=False)
    print(f"Edgelist saved to {output_file}")

class FunctionCallAnalyzer(ast.NodeVisitor):
    def __init__(self, filename):
        self.filename = filename
        self.defined_functions = set()
        self.calls = defaultdict(set)
        self.current_function = None

    def visit_FunctionDef(self, node):
        func_name = f"{self.filename}:{node.name}"
        self.defined_functions.add(func_name)
        prev_function = self.current_function
        self.current_function = func_name
        self.generic_visit(node)
        self.current_function = prev_function

    def visit_Call(self, node):
        if isinstance(node.func, ast.Name):
            if self.current_function:
                self.calls[self.current_function].add(node.func.id)
        elif isinstance(node.func, ast.Attribute):
            if self.current_function:
                self.calls[self.current_function].add(node.func.attr)
        self.generic_visit(node)

def find_python_files(folder):
    for root, dirs, files in os.walk(folder):
        dirs[:] = [d for d in dirs if d not in EXCLUDED_DIRS]
        for file in files:
             if file.endswith('.py') and not any(fnmatch.fnmatch(file, pattern) for pattern in EXCLUDED_FILES):
                yield os.path.join(root, file)
                print (f"Found Python file: {file} in {root}")


def analyze_folder(folder):
    analyzer = FunctionCallAnalyzer
    all_defined = set()
    all_calls = defaultdict(set)

    for filepath in find_python_files(folder):
        with open(filepath, "r", encoding="utf-8") as f:
            try:
                print(f"Analyzing {filepath}")
                tree = ast.parse(f.read(), filename=filepath)
                visitor = analyzer(filepath)
                visitor.visit(tree)
                all_defined.update(visitor.defined_functions)
                for k, v in visitor.calls.items():
                    all_calls[k].update(v)
            except SyntaxError:
                print(f"Skipping {filepath}, syntax error.")
                continue

    return all_defined, all_calls

def build_graph(defined_funcs, func_calls):
    G = nx.DiGraph()
    for func in defined_funcs:
        G.add_node(func)
    for caller, callees in func_calls.items():
        for callee in callees:
            matched = [f for f in defined_funcs if f.endswith(f":{callee}")]
            for callee_func in matched:
                G.add_edge(caller, callee_func)
    return G

def draw_graph(G, output_path="function_call_graph.png"):
    plt.figure(figsize=(12, 12))
    pos = nx.spring_layout(G, k=0.3)
    nx.draw(G, pos, with_labels=True, node_size=2000, node_color="skyblue", edge_color="gray", font_size=8)
    plt.title("Function Call Graph")
    plt.savefig(output_path, bbox_inches="tight")
    plt.show()

if __name__ == "__main__":
    import sys
    folder = sys.argv[1] if len(sys.argv) > 1 else "."
    print(f"Analyzing folder: {folder}")
    defined, calls = analyze_folder(folder)
    graph = build_graph(defined, calls)
    draw_graph(graph)
    save_edgelist_to_excel(graph)

