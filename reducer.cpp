#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include "RTree.h"
#include <geos/io/WKTReader.h>
#include <geos/geom/Point.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/Geometry.h>

using namespace std;
using namespace geos::geom;
using namespace geos::io;

//存放map传入的id与wkt数据
struct PolygonData {
    int id;
    shared_ptr<Polygon> polygon;
};

int main() {
    string line;
    RTree<PolygonData*, double, 2> rtree;
    map<int, int> pointCountMap;//记录点数
    WKTReader wktReader;
    vector<string> points;//记录点，用于循环
    map<int, shared_ptr<Polygon>> polygonMap;

    while (getline(cin, line)) {


        stringstream ss(line);
        string key, value;
        getline(ss, key, '\t'); 
        getline(ss, value); //拆分键值对


        stringstream valueStream(value); 
        string part;
        getline(valueStream, part, ',');//id
        int id = stoi(part);//id为整数
        getline(valueStream, part);//wkt字符串
        string wkt = part;

        if (wkt.find("POLYGON") == 0) {
            auto geom = shared_ptr<Geometry>(wktReader.read(wkt));//转化为多边形数据
            auto polygon = dynamic_pointer_cast<Polygon>(geom);
            if (polygon) {
                const Envelope* env = polygon->getEnvelopeInternal();//获取mbr
                double min[2] = {env->getMinX(), env->getMinY()};
                double max[2] = {env->getMaxX(), env->getMaxY()};
                PolygonData* pd = new PolygonData{id, polygon};
                rtree.Insert(min, max, pd);//插入rtree
                polygonMap[id] = polygon;
            }
        } else if (wkt.find("POINT") == 0) {
            points.push_back(wkt);//存储点用于遍历
        }
    }

    for (const auto& wkt : points) {
        auto geom = shared_ptr<Geometry>(wktReader.read(wkt));
        auto point = dynamic_pointer_cast<Point>(geom);
        if (point) {
            double min[2] = {point->getX(), point->getY()};
            double max[2] = {point->getX(), point->getY()};//获取点坐标
            rtree.Search(min, max, [&pointCountMap, &polygonMap, &point](PolygonData* const& pd) -> bool {//rtree过滤
                if (pd->polygon->contains(point.get())) {//判断包含关系
                    pointCountMap[pd->id]++;
                }
                return true; 
            });
        }
    }

    for (const auto& entry : pointCountMap) {
        cout << "Polygon ID: " << entry.first << ", Points count: " << entry.second << endl;
    }

    rtree.RemoveAll();

    return 0;
}
