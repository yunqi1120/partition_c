#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <chrono>
#include "RTree.h"
#include <geos/io/WKTReader.h>
#include <geos/geom/Point.h>
#include <geos/geom/Polygon.h>
#include <geos/geom/Geometry.h>
#include <cstdlib>

using namespace std;
using namespace geos::geom;
using namespace geos::io;
using namespace std::chrono;

struct PolygonData {
    int id;
    shared_ptr<Polygon> polygon;
};


int main() {

    auto startTime = high_resolution_clock::now();

    string line;
    RTree<PolygonData*, double, 2> rtree;
    map<int, int> pointCountMap;
    WKTReader wktReader;
    vector<string> points;
    map<int, shared_ptr<Polygon>> polygonMap;

    microseconds totalWKTReadTime(0);
    microseconds totalRTreeBuildTime(0);
    microseconds totalRTreeSearchTime(0);
    microseconds totalContainsTime(0);
    microseconds totalIOTime(0);//标准输入行
    //microseconds totalPushBackTime(0); //用于累计push_back操作的总时间
    //microseconds forLoopDuration(0);

    string currentKey;
    while (true) {

    auto ioStart1 = high_resolution_clock::now(); 
        if (!getline(cin, line)) {
        break;
    }

        //分离key与value
        stringstream ss(line);
        string key, value;
        getline(ss, key, '\t');
        getline(ss, value);

    auto ioEnd1 = high_resolution_clock::now(); 
    totalIOTime += duration_cast<microseconds>(ioEnd1 - ioStart1); 

        //当key值发生变化时进行点与多边形的判断
        if (!currentKey.empty() && key != currentKey) {
            
            for (const auto& wkt : points) {

                auto wktReadStart = high_resolution_clock::now();

                //wktreader转化成geos可以处理的格式
                auto geom = shared_ptr<Geometry>(wktReader.read(wkt));

                auto wktReadEnd = high_resolution_clock::now();
                totalWKTReadTime += duration_cast<microseconds>(wktReadEnd - wktReadStart);

                auto point = dynamic_pointer_cast<Point>(geom);
                double min[2] = {point->getX(), point->getY()};
                double max[2] = {point->getX(), point->getY()};

                auto rTreeSearchStart = high_resolution_clock::now();

                //rtree过滤
                rtree.Search(min, max, [&pointCountMap, &point, &totalContainsTime](PolygonData* const& pd) -> bool {
                
                auto containsStart = high_resolution_clock::now(); 

                    //geos判断
                    if (pd->polygon->contains(point.get())) {
                        pointCountMap[pd->id]++;
                    }
                auto containsEnd = high_resolution_clock::now();
                totalContainsTime += duration_cast<microseconds>(containsEnd - containsStart);

                    return true;
                });
                auto rTreeSearchEnd = high_resolution_clock::now();
                totalRTreeSearchTime += duration_cast<microseconds>(rTreeSearchEnd - rTreeSearchStart);
            }
            points.clear(); 

            //auto forLoopStart = high_resolution_clock::now();

            //输出结果
            for (const auto& entry : pointCountMap) {
                cout << "Polygon ID: " << entry.first << ", Points count: " << entry.second << endl;
            }

            //auto forLoopEnd = high_resolution_clock::now(); 
            //forLoopDuration += duration_cast<microseconds>(forLoopEnd - forLoopStart); 

            //清空rtree
            rtree.RemoveAll();
            pointCountMap.clear();
            currentKey = key;
        }

        //key值没有变化时正常建立rtree
        currentKey = key;
        //分离value中的不同部分
        auto ioStart2 = high_resolution_clock::now(); 
        stringstream valueStream(value);
        string idStr, wkt;
        getline(valueStream, idStr, ',');
        getline(valueStream, wkt);
        int id = stoi(idStr);

        auto ioEnd2 = high_resolution_clock::now(); 
        totalIOTime += duration_cast<microseconds>(ioEnd2 - ioStart2); 

        if (wkt.substr(0, 3) == "POL") {

            auto wktReadStart = high_resolution_clock::now();

            auto geom = shared_ptr<Geometry>(wktReader.read(wkt));

            auto wktReadEnd = high_resolution_clock::now();
            totalWKTReadTime += duration_cast<microseconds>(wktReadEnd - wktReadStart);

            auto rTreeBuildStart = high_resolution_clock::now();

            auto polygon = dynamic_pointer_cast<Polygon>(geom);
            const Envelope* env = polygon->getEnvelopeInternal();
            double min[2] = {env->getMinX(), env->getMinY()};
            double max[2] = {env->getMaxX(), env->getMaxY()};
            PolygonData* pd = new PolygonData{id, polygon};

            rtree.Insert(min, max, pd);

            auto rTreeBuildEnd = high_resolution_clock::now();
            totalRTreeBuildTime += duration_cast<microseconds>(rTreeBuildEnd - rTreeBuildStart);

        } else if (wkt.substr(0, 3) == "POI") {

            //auto pushBackStart = high_resolution_clock::now(); 
            points.push_back(wkt);
            //auto pushBackEnd = high_resolution_clock::now();
            //totalPushBackTime += duration_cast<microseconds>(pushBackEnd - pushBackStart); 

        }
    }

//最后一个key需要单独处理
    if (!points.empty()) {

            for (const auto& wkt : points) {

                auto wktReadStart = high_resolution_clock::now();

                auto geom = shared_ptr<Geometry>(wktReader.read(wkt));

                auto wktReadEnd = high_resolution_clock::now();
                totalWKTReadTime += duration_cast<microseconds>(wktReadEnd - wktReadStart);

                auto point = dynamic_pointer_cast<Point>(geom);
                double min[2] = {point->getX(), point->getY()};
                double max[2] = {point->getX(), point->getY()};

                auto rTreeSearchStart = high_resolution_clock::now();

                rtree.Search(min, max, [&pointCountMap, &point, &totalContainsTime](PolygonData* const& pd) -> bool {
                
                auto containsStart = high_resolution_clock::now();    
                    if (pd->polygon->contains(point.get())) {
                        pointCountMap[pd->id]++;
                    }
                auto containsEnd = high_resolution_clock::now();
                totalContainsTime += duration_cast<microseconds>(containsEnd - containsStart);

                    return true;
                });
                auto rTreeSearchEnd = high_resolution_clock::now();
                totalRTreeSearchTime += duration_cast<microseconds>(rTreeSearchEnd - rTreeSearchStart);
            }
            points.clear(); 

            //auto forLoopStart = high_resolution_clock::now(); // 记录for循环开始时间

            for (const auto& entry : pointCountMap) {
                cout << "Polygon ID: " << entry.first << ", Points count: " << entry.second << endl;
            }

            //auto forLoopEnd = high_resolution_clock::now(); // 记录for循环结束时间
            //forLoopDuration += duration_cast<microseconds>(forLoopEnd - forLoopStart); 

    }

    auto endTime = high_resolution_clock::now();

    cerr << "Reducer WKTReader Time: " << totalWKTReadTime.count() / 1000000.0 << "s" << endl;
    cerr << "Reducer R-Tree Build Time: " << totalRTreeBuildTime.count() / 1000000.0 << "s" << endl;
    //总时间-精炼时间=使用rtree进行mbr的过滤时间
    cerr << "Reducer R-Tree Search Time: " << (totalRTreeSearchTime.count() - totalContainsTime.count()) / 1000000.0 << "s" << endl;
    cerr << "Reducer Contains Check Time: " << totalContainsTime.count() / 1000000.0 << "s" << endl;
    cerr << "Reducer IO Read Time: " << totalIOTime.count() / 1000000.0 << "s" << endl;
    //cerr << "Reducer Forloop Cout time:" << forLoopDuration.count() / 1000000.0 << " s" << endl; 
    //cerr << "Reducer push_back points Time: " << totalPushBackTime.count() / 1000000.0 << "s" << endl;
    cerr << "Reducer Total Reducer Time: " << duration_cast<microseconds>(endTime - startTime).count() / 1000000.0 << "s" << endl;

    return 0;
}
