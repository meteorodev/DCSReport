import random
import string
class PassGen:

    def __init__(self, len_pass=12, inc_spec_chart=True):
        self.len_pass = len_pass
        self.inc_spec_chart = inc_spec_chart

    def make_ran_pass(self):

        if self.inc_spec_chart:
            chars_all = string.ascii_letters + string.digits + string.punctuation
        else:
            chars_all = string.ascii_letters + string.digits

        chars = [random.choice(chars_all) for i in range(self.len_pass)]

        passw = "".join(chars)
        passw = passw.replace('%',"+")
        passw = passw.replace("'", "*")
        passw = passw.replace('"', "-")
        return passw


if __name__ == '__main__':
    ps = PassGen()

    print(ps.make_ran_pass())