from requests import get
from requests.exceptions import RequestException
from contextlib import closing


def is_good_response(resp):
    """
    Returns True if the response seems to be HTML, False otherwise.
    """
    content_type = resp.headers['Content-Type'].lower()
    return (resp.status_code == 200
            and content_type is not None
            and content_type.find('html') > -1)


def log_error(e):
    """
    It is always a good idea to log errors.
    This function just prints them, but we can
    make it do anything.

    """
    print(e)


def scrap(url):
    """
    Attempts to get the content at `url` by making an HTTP GET request.
    If the content-type of response is some kind of HTML/XML, return the
    text content, otherwise return None.

    The closing() function ensures that any network resources are freed when
    they go out of scope in that with block.
    Using closing() like that is good practice and helps to prevent
    fatal errors and network timeouts.

    """
    try:
        with closing(get(url, stream=True)) as resp:
            if is_good_response(resp):
                return resp.content
            else:
                log_error('Error accessing access the Url {0}'.format(url))
                return None

    except RequestException as e:
        log_error('Error during requests to {0} : {1}'.format(url, str(e)))
        return None

