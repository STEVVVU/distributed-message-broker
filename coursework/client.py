"""Portable topic/channel client. A receipt belongs to its topic and channel."""
import json
import urllib.request
import urllib.parse
import urllib.error


class Broker:
    def __init__(self, url, token, observer=None):
        self.url = url.rstrip('/')
        self.token = token
        self.observer = observer

    def request(self, method, path, data=None):
        encoded = None if data is None else json.dumps(data).encode()
        request = urllib.request.Request(self.url + path, data=encoded, method=method,
            headers={'Authorization': 'Bearer ' + self.token, 'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(request, timeout=6) as response:
                result = json.load(response)
        except urllib.error.HTTPError as error:
            if self.observer:
                self.observer('http_error', method=method, path=path, request=data, status=error.code)
            raise
        if self.observer:
            self.observer('http', method=method, path=path, request=data, response=result)
        return result

    @staticmethod
    def path(topic, channel=None):
        path = '/topics/' + urllib.parse.quote(topic, safe='')
        return path if channel is None else path + '/channels/' + urllib.parse.quote(channel, safe='')

    def topic(self, topic):
        return self.request('PUT', self.path(topic))

    def channel(self, topic, channel):
        return self.request('PUT', self.path(topic, channel))

    def publish(self, topic, data, publisher_id):
        return self.request('POST', self.path(topic) + '/messages', dict(data=data, publisher_id=publisher_id))

    def pull(self, topic, channel):
        return self.request('POST', self.path(topic, channel) + '/pull')['messages']

    def ack(self, topic, channel, receipt):
        return self.request('POST', self.path(topic, channel) + '/ack', dict(receipt=receipt))


if __name__ == '__main__':
    import os
    import sys
    if len(sys.argv) != 5 or sys.argv[1] != 'pull':
        raise SystemExit('usage: client.py pull URL TOPIC CHANNEL; set BROKER_TOKEN')
    print(json.dumps(Broker(sys.argv[2], os.environ['BROKER_TOKEN']).pull(sys.argv[3], sys.argv[4])))
