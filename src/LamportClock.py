
# Lamport clock template
class LamportClock:
    #: The clocks current value.
    value = 0

    def __init__(self, initial_value=0):
        self.value = initial_value

    def adjust(self, other):
        self.value = max(self.value, other)

    def forward(self):
        self.value += 1
        return self.value