from tests.conftest import SUCCESSFUL_RUN_MSG


def test_bronze_pipeline(session, context, caplog):
    session.run(node_names=["bronze.bgee"])
    assert SUCCESSFUL_RUN_MSG in caplog.text
