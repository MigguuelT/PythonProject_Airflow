from airflow.sdk import BaseOperator


class MaskCsvOperator(BaseOperator):

    def __init__(self, input_file: str, output_file: str, separator: str, column: str, **kwargs: object) -> None:
        super().__init__(**kwargs)
        self.input_file = input_file
        self.output_file = output_file
        self.separator = separator
        self.column = column

    def execute(self, context):
        file = ''
        with (open(self.input_file, 'r') as f):
            for data in f.readlines():
                fields = data.strip('\n').split(self.separator)
                fields[self.column] = '******'
                file += self.separator.join(fields) + '\n'

        with open(self.output_file, 'w') as f:
            f.write(file)
