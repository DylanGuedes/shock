# encoding=utf8
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import utm

from scipy import spatial
from xml.dom import minidom

def load_nodes_from_xml(content):
    """
    Extract x,y from every node in a xml string

    >>> load_nodes_from_xml("<node id='1001237099' x='2819741.636735585' y='7204646.500622427'/>")
    [[2819741.636735585, 7204646.500622427]]
    """
    nodes = content.getElementsByTagName('node')
    reduce(lambda x, y: y.append([x.getAttribute("x"), x.getAttribute("y")]), nodes, [])


def coordinates_from_xml(path):
    """
    Load coordinates from xml file

    >>> coordinates_from_xml("map.xml")
    [[2819741.636735585, 7204646.500622427]]
    """
    minidom.parse(path)


def closest_point(kd_tree, point, points):
    distance, closest_neighbor = kd_tree.query([point])
    idx = closest_neighbor[0]
    return points[idx]


def mount_kd_tree(points):
    return spatial.KDTree(points)


def olho_vivo_get_auth(token):
    url = "http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token={0}"\
            .format(token)
    cookies = requests.post(url).cookies
    return cookies.get_dict()['apiCredentials']


def request_buses(auth):
    url = "http://api.olhovivo.sptrans.com.br/v2.1/Posicao"
    resp = requests.get(url, cookies={"apiCredentials": auth})
    return json.loads(resp.text)


def geo_to_xy(coordinate):
    x, y, _, _ = utm.from_latlon(coordinate["px"], coordinate["py"])
    return [x,y]


def bus_data_from_geo_to_xy(data):
    return map(geo_to_xy, data)


auth = olho_vivo_get_auth(mytoken)
buses_data = request_buses(auth)

transformed_buses = bus_data_from_geo_to_xy(buses_data['l'])


pts = coordinates_from_xml("map.xml")
tree = mount_kd_tree(pts)
closest_point(tree, mypoint, pts)
