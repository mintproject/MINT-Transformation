import click
from dtran.config_parser import ConfigParser


@click.group(invoke_without_command=False)
def cli():
    pass


@cli.command(name="create_pipeline")
@click.option("--config", help="full path to config")
def create_pipeline(config):
    parser = ConfigParser(config)
    parsed_pipeline, parsed_inputs = parser.parse()

    # Validate parsed info
    parsed_pipeline.validate()

    # Execute the pipeline
    outputs = parsed_pipeline.exec(parsed_inputs)


@cli.command(name="config_parser")
@click.option("--config", help="full path to config")
def config_parser():
    click.echo("This is config parser!")


if __name__ == "__main__":
    cli()