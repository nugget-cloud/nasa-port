# Complete NASA Exoplanet Data Pipeline Example
from nasa_port.data_bindings import NASAPipeline
from nasa_port.builder.query_builder import QueryBuilder
from nasa_port.builder.models import TableName

def run_pipeline():
    # 1. Configure pipeline
    db_path = Path("pipeline_output/discoveries.duckdb")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    pipeline = NASAPipeline.for_local_development(
        pipeline_name="recent_discoveries",
        data_dir="pipeline_output"
    )
    pipeline.config.destination_params["database_path"] = str(db_path)
    
    # 2. Build query for 10 most recent discoveries
    recent_planets_query = (
        QueryBuilder()
        .select(['pl_name', 'hostname', 'discoverymethod', 
                'disc_year', 'sy_dist'])
        .from_table(TableName.PLANETARY_SYSTEMS)
        .where_confirmed()
        .order_by('disc_year', ascending=False)
        .limit(10)
    )
    
    load_info = pipeline.load_custom_query(
        query=recent_planets_query,
        resource_name="recent_discoveries"
    )
    
    run_pipeline()