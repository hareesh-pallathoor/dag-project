from dag_project.dag import DAGTask

def func():
    print("Hello")

def test_dag_project():
    dag = DAGTask("Test")
    dag.add_task("A", func)
    dag.add_task("B", func, dependencies=["A"])
    dag.add_task("F", func, dependencies=["D"])
    dag.add_task("C", func, dependencies=["A"])
    dag.add_task("D", func, dependencies=["B", "C"])
    dag.add_task("E", func, dependencies=["B"])
    dag.add_task("H", func, dependencies=["E","F","L"])
    dag.add_task("L", func, dependencies=["A","C"])
    dag.run("L")

def test_only_required_tasks_run(capsys):
    dag = DAGTask("Test")
    dag.add_task("A")
    dag.add_task("B", func, dependencies=["A"])
    dag.add_task("C", func, dependencies=["A"])
    dag.run("C")
    captured = capsys.readouterr()
    assert "Executing node: B" not in captured.out
    assert "Executing node: A" in captured.out