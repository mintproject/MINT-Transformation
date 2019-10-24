import click

from dtran.config_parser import ConfigParser


@click.group(invoke_without_command=False)
def cli():
    pass


@cli.command(name="create_pipeline", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
))
@click.option("--config", help="Full path to the config.")
@click.option(
    "--dryrun", is_flag=True,
    help="Only check parsed inputs without actual execution."
)
@click.pass_context
def create_pipeline(ctx, config=None, dryrun=False):
    """
    Creates a pipeline and execute it based on given config and input(optional).
    To specify the input to pipeline, use (listed in ascending priority):
    1) config file option: --config path_to_file
    2) arg params: e.g. --FuncName.Attr=value
    """

    # Accept user-specified inputs: expect format of --key=value
    user_inputs = {}
    for arg in ctx.args:
        try:
            key, value = arg[2:].split("=")
            func_name, attr_name = key.split(".")
            user_inputs[(func_name, attr_name)] = value
        except ValueError:
            print(f"user input: '{arg}' should have format '--FuncName.Attr=value'")
            return

    parser = ConfigParser(user_inputs)
    parsed_pipeline, parsed_inputs = parser.parse(config)

    if dryrun:
        print(parsed_inputs)
        return

    # Execute the pipeline
    parsed_pipeline.exec(parsed_inputs)


@cli.command(name="config_parser")
@click.option("--config", help="full path to config")
def config_parser():
    click.echo("This is config parser!")


if __name__ == "__main__":
    cli()