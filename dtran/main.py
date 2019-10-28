import click

from dtran.config_parser import ConfigParser


@click.group(invoke_without_command=False)
def cli():
    pass


@cli.command(name="create_pipeline", context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
))
@click.option("--config", help="full path to config")
@click.pass_context
def create_pipeline(ctx, config=None):
    """
    Creates a pipeline and execute it based on given config and input(optional).
    To specify the input to pipeline, use (listed in ascending priority):
    1) config file option: --config path_to_file
    2) arg params: e.g. --FuncName.Attr=value
    """
    parser = ConfigParser()
    parsed_pipeline, parsed_inputs, mappings = parser.parse(config)

    # Accept user-specified inputs: expect format of --key=value
    for arg in ctx.args:
        key, value = arg[2:].split("=")
        func_name, attr_name = key.split(".")
        if func_name in mappings:
            parsed_inputs[mappings[func_name] + attr_name] = value

    # Execute the pipeline
    parsed_pipeline.exec(parsed_inputs)


@cli.command(name="config_parser")
@click.option("--config", help="full path to config")
def config_parser():
    click.echo("This is config parser!")


if __name__ == "__main__":
    cli()