def job_params(dbutils):
    all_args = dict(dbutils.notebook.entry_point.getCurrentBindings())
    # remove '--' substring
    all_args = {key.replace('--', ''): value for key, value in all_args.items()}
    # parse values to correct format
    all_args = {key: ast.literal_eval(value) for key, value in all_args.items()}
    return all_args
