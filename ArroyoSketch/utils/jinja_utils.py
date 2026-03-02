from jinja2 import Environment, FileSystemLoader, nodes


def load_template(template_dir, template_name):
    """Load a template from the specified directory"""
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template(template_name)
    return template


def get_template_variables(template_source, environment=None):
    """
    Extract all template variables from a Jinja2 template source.

    Args:
        template_source (str): The raw template source code
        environment (Environment, optional): Jinja2 environment. If None, creates a default one.

    Returns:
        set: Set of variable names found in the template
    """
    if environment is None:
        environment = Environment()

    ast = environment.parse(template_source)
    template_vars = ast.find_all(nodes.Name)
    return {var.name for var in template_vars if var.ctx == "load"}
