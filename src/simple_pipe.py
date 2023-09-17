from pathlib import Path
import get
import database
import env_setup
import logging
import tomllib

if __name__ == "__main__":
    print("Searching for my pipe....")
    failed_tasks = 0

    try:
        with open("config/pipeline.toml", "rb") as f:
            pipe_cfg = tomllib.load(f)
    except Exception as e:
        print(f"*** Where's the pipe? ***\n{e}")
        raise

    log_file = pipe_cfg["logging"]["logfile"]
    log_path = pipe_cfg["logging"]["log_folder"]
    log_file = log_path + log_file
    try:
        Path(log_path).mkdir(exist_ok=True)
    except Exception as e:
        print(e)
        raise

    print(f"Making notes in {log_file}")

    logging.basicConfig(
        filename=log_file,
        format="%(asctime)s %(levelname)s: %(message)s",
        level=pipe_cfg["logging"]["level"],
        datefmt="%d/%m/%Y %H:%M:%S",
        filemode="w",
    )
    logging.info(f"Simple Pipe: Lighting the fire!\n{'*'*100}")
    print("Lighting my pipe...")

    pipeline = pipe_cfg["pipeline"]
    logging.info(f'Found pipeline: {pipeline["name"]}')
    logging.info(f'Pipeline Description: {pipeline["description"]}')

    logging.info("Loading environment variables")
    env_setup.load()

    logging.info(
        f"Pipeline Config: schema: {pipeline['schema']} , database engine: {pipeline['database']}"
    )


    db_name = pipe_cfg[pipeline["database"]]["credentials"]["database"]
    db_path = pipe_cfg[pipeline["database"]]["credentials"]["path"]


    logging.info(f"Connecting to database {pipeline['database']}: {db_path}{db_name} ")
    try:
        db_con = database.connect(pipeline["database"], db_name, dbpath=db_path)
    except Exception as e:
        print(e)
        raise

    logging.info("Simple Pipe: Smoking!")
    logging.info("Simple Pipe: Looking at task list....")
    tasks = pipe_cfg["task"]
    for task in tasks:
        if tasks[task]["active"]:
            logging.info(f"- Task: {task} type: {tasks[task]['file_type']}")

    logging.info("Simple Pipe: Processing Task List")

    for task in tasks:
        if tasks[task]["active"]:
            logging.info(f"Looking at task: {task} type: {tasks[task]['file_type']}")

            if tasks[task]["file_type"] == "excel":
                df_upload = get.excel(
                    tasks[task]["url"],
                    tasks[task]["workbook"],
                    tasks[task]["skiprows"],
                    tasks[task]["columns"],
                )

            elif tasks[task]["file_type"] == "csv":
                df_upload = get.csv(tasks[task]["url"])

            else:
                logging.info(
                    f'- Oops problem with the task "{task}". The task type {tasks[task]["file_type"]} is not support (yet!)'
                )
                failed_tasks += 1

            if df_upload is None:
                logging.error(f"- {task}: No raw data extracted")
                failed_tasks += 1
            else:
                sql_table = tasks[task]["sql_table"]
                sql_filter_name = tasks[task]["sql_filter"]
                if sql_filter_name:
                    sql_filter = pipe_cfg["sql"][sql_filter_name]["sql"]
                sql_write = tasks[task]["sql_write"]

                if sql_filter_name:
                    logging.info(f"- SQL filtering: {sql_filter_name}")
                    df_upload = get.sqlfilter(db_con, df_upload, sql_filter)
                    if df_upload is None:
                        failed_tasks +=1

                database.df_load(
                    db_con,
                    df_upload,
                    sql_table,
                    sqlschema=pipeline["schema"],
                    sqlwrite=sql_write,
                )

            logging.info(f"Finished processing task: {task}")

    if failed_tasks:
        fail_txt = "Oh No!"
    else:
        fail_txt = "Yay!"

    print(f"{fail_txt} Pipe went out this many times: {failed_tasks}")
    logging.info(f"Simple Pipe: {fail_txt} {failed_tasks} tasks failed!")
    logging.info(f"Simple Pipe: Smoked!\n{'*'*100}")
    print("Yay! Pipe smoked!")
