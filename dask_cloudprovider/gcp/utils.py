import httplib2
import googleapiclient.http
import google_auth_httplib2


def build_request(credentials=None):
    def inner(http, *args, **kwargs):
        new_http = httplib2.Http()
        if credentials is not None:
            new_http = google_auth_httplib2.AuthorizedHttp(credentials, http=new_http)

        return googleapiclient.http.HttpRequest(new_http, *args, **kwargs)

    return inner
