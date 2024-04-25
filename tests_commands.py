import click

@click.command()
@click.option("-first_name","--f_name")
def func(f_name):

    print(f_name)


if __name__ == "__main__":

    func()
