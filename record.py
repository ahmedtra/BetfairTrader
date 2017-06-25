from authenticate import authenticate
from data.quote_recorder import Recorder


if __name__ == '__main__':
    client = authenticate()

    recorder = Recorder(client, [1])

    recorder.looper()
