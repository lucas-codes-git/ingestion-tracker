async def retry_bronze_job(
    tracker,
    supabase,
    file,
    bucket_name
):
    ingestion_id = file["ingestion_id"]
    batch_id = file["batch_id"]
    file_path = file["file_path"]

    try:
        file_bytes = supabase.download_file(
            bucket_name=bucket_name,
            file_path=file_path
        )

        started = await tracker.start_bronze_job(
            ingestion_id
        )

        if not started:
            return False

        supabase.upload_file(
            bucket_name=bucket_name,
            source_name=file["source_system"],
            data_name=file["endpoint"],
            clean_raw=False,
            file_name=batch_id,
            file_bytes=file_bytes,
            file_type=file["file_type"]
        )

        await tracker.complete_bronze_job(
            ingestion_id
        )

        return True

    except Exception as e:

        await tracker.fail_bronze(
            ingestion_id,
            str(e)
        )

        raise