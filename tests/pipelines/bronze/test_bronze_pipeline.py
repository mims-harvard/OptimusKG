from optimuskg.pipelines.bronze import create_pipeline as create_bronze_pipeline


def test_bronze_pipeline(project_context):
    pipeline = create_bronze_pipeline()
    pipeline.run(project_context)
