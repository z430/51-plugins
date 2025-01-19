import fiftyone.operators as foo
import fiftyone.operators.types as types

class SimpleInputExample(foo.Operator):
    @property
    def config(self):
        return foo.OperatorConfig(
            name="simple_input_example",
            label="Simple input example",
        )

    def resolve_input(self, ctx):
        inputs = types.Object()
        inputs.str("message", label="Message", required=True)
        header = "Simple input example"
        return types.Property(inputs, view=types.View(label=header))

    def execute(self, ctx):
        return {"message": ctx.params["message"]}

    def resolve_output(self, ctx):
        outputs = types.Object()
        outputs.str("message", label="Message")
        header = "Simple input example: Success!"
        return types.Property(outputs, view=types.View(label=header))

def register(p):
    p.register(SimpleInputExample)
