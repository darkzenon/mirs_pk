from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin


app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

import psycopg2
import json
import base64


PATH_output = 'C:\\Users\\denis\\python\\hack_pkk\\presentaton\\imgs\\'

def db_connect():
    con = psycopg2.connect(dbname = "reestr", user='postgres', password='rossgress', port=9999, host='localhost')

    return con

def db_disconnect(con):
    con.close()

def get_top_agri():
	connect = db_connect()
	cursor = connect.cursor()
	result_list = {}
	command = (f"SELECT attrs_cn, util_code, category_type, center_x, center_y, extent_xmin, extent_xmax, extent_ymin, extent_ymax, ndvi, attrs_id FROM objects_selhoz LIMIT 10")
	cursor.execute(command)
	for row in cursor:
		result_list[row[0]] = {"util_code": str(row[1]), "category_type": str(row[2]), "y": int(float(row[3])), "x": int(float(row[4])), "ymin": int(float(row[7])), "ymax": int(float(row[8])), "xmin": int(float(row[5])), "xmax": int(float(row[6]))}
	cursor.close()
	db_disconnect(connect)
	return result_list

def get_odject_detail(uuid):
	connect = db_connect()
	cursor = connect.cursor()
	command = (f"SELECT attrs_id, attrs_kvartal_cn, attrs_address, attrs_cad_cost, attrs_cc_date_entering, attrs_area_value, attrs_util_code, attrs_util_by_doc, attrs_cn, attrs_cad_unit  FROM objects_rosreestr WHERE attrs_cn = %s")
	data = (str(uuid), )
	cursor.execute(command, data)
	result_list = {}

	for i, row in enumerate(cursor):
		result_list["object"] = {'attrs_id': row[0], 'attrs_kvartal_cn': row[1], 'attrs_address': row[2], 'attrs_cad_cost': row[3], 'attrs_cc_date_entering': row[4], 'attrs_area_value': row[5], 'attrs_util_code': row[6], 'attrs_util_by_doc': row[7], 'cad_unit': row[8]}
	cursor.close()
	db_disconnect(connect)
	
	result_list['imgs'] = select_image('img_rr_cut', uuid)
	
	return result_list

def select_image(map_col, cn):
	connect = db_connect()
	cursor = connect.cursor()
	command = (f"SELECT {map_col} FROM map_images WHERE attrs_cn = '{cn}'")
	cursor.execute(command)
	for row in cursor:
		data64 = row[0]
		image = base64.b64decode(data64)
	return data64
	cursor.close()
	db_disconnect(connect)


@app.route('/img/', methods=['GET'])
def get_img():
	image = select_image('img_rr_cut','59:10:0501003:98')
	return image
	

@app.route('/list/', methods=['GET'])
def get_new_objects():
	#get_top_agri()
	return get_top_agri()


@app.route('/object/', methods=['POST'])
def get_object_detail():
	content = request.get_json(silent=True, force=False)
	data = get_odject_detail(content['uuid'])
	return jsonify(data)




@app.route('/listh/', methods=['POST', 'GET'])
def get_new_objectsg():
	content = request.get_json(silent=True, force=False)
	print(content)
	return jsonify(request.json)


@app.route('/')
def index():
    return "Hello, World!"

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=9922)
