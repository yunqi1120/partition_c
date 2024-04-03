#include <fstream>
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

//存放map传入的id与wkt数据
struct PolygonData {
    int id;
    shared_ptr<Polygon> polygon;
};

int main() {
    auto startTime = high_resolution_clock::now();

    const char* taskId = getenv("mapreduce_task_id");
    string taskIdStr = taskId ? taskId : "Unknown Task ID";

    string line;
    RTree<PolygonData*, double, 2> rtree;
    map<int, int> pointCountMap;//记录点数
    WKTReader wktReader;
    vector<string> points;//记录点，用于循环
    map<int, shared_ptr<Polygon>> polygonMap;
    microseconds totalWKTReadTime(0);
    microseconds totalRTreeBuildTime(0);
    microseconds totalRTreeSearchTime(0);
    microseconds totalContainsTime(0);
    microseconds totalIOTime(0);//标准输入行
    microseconds totalPushBackTime(0); //用于累计push_back操作的总时间


    while (true) {

        auto ioStart = high_resolution_clock::now(); 

        if (!getline(cin, line)) { 
            break;
        }

        auto ioEnd = high_resolution_clock::now(); 
        //读入map输出文件
        totalIOTime += duration_cast<microseconds>(ioEnd - ioStart); 

        auto wktReadStart = high_resolution_clock::now();

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

        shared_ptr<Geometry> geom = wktReader.read(wkt);

        auto wktReadEnd = high_resolution_clock::now();
        //wkt转换到geos对应格式
        totalWKTReadTime += duration_cast<microseconds>(wktReadEnd - wktReadStart);

        if (wkt.substr(0, 3) == "POL") {//对比前三位判断是POLYGON还是POINT

            auto rTreeBuildStart = high_resolution_clock::now();
            auto polygon = dynamic_pointer_cast<Polygon>(geom);
            const Envelope* env = polygon->getEnvelopeInternal();//获取mbr
            double min[2] = {env->getMinX(), env->getMinY()};
            double max[2] = {env->getMaxX(), env->getMaxY()};
            PolygonData* pd = new PolygonData{id, polygon};
            rtree.Insert(min, max, pd);//插入rtree
            polygonMap[id] = polygon;
            auto rTreeBuildEnd = high_resolution_clock::now();
            //建立rtree时间
            totalRTreeBuildTime += duration_cast<microseconds>(rTreeBuildEnd - rTreeBuildStart);

        } else if (wkt.substr(0, 3) == "POI") {
            
            auto pushBackStart = high_resolution_clock::now(); 
            points.push_back(wkt);
            auto pushBackEnd = high_resolution_clock::now();
            //存储点的时间
            totalPushBackTime += duration_cast<microseconds>(pushBackEnd - pushBackStart); 
        }
    }

    for (const auto& wkt : points) {
        auto wktReadStart = high_resolution_clock::now();
        auto geom = shared_ptr<Geometry>(wktReader.read(wkt));
        auto wktReadEnd = high_resolution_clock::now();
        //wkt转换到geos对应格式
        totalWKTReadTime += duration_cast<microseconds>(wktReadEnd - wktReadStart);

        auto point = dynamic_pointer_cast<Point>(geom);
        double min[2] = {point->getX(), point->getY()};
        double max[2] = {point->getX(), point->getY()};

        auto rTreeSearchStart = high_resolution_clock::now();
        rtree.Search(min, max, [&pointCountMap, &polygonMap, &point, &totalContainsTime](PolygonData* const& pd) -> bool {
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

    auto forLoopStart = high_resolution_clock::now(); // 记录for循环开始时间

    for (const auto& entry : pointCountMap) {
        cout << "Polygon ID: " << entry.first << ", Points count: " << entry.second << endl;
    }

    auto forLoopEnd = high_resolution_clock::now(); // 记录for循环结束时间
    auto forLoopDuration = duration_cast<microseconds>(forLoopEnd - forLoopStart).count(); // 计算for循环持续时间


    rtree.RemoveAll();

    auto endTime = high_resolution_clock::now();

    //日志形式记录结果
    cerr << "Reducer WKT Read Time: " << totalWKTReadTime.count() / 1000.0 << "us"<< "     " << "reducer_id" << taskIdStr << endl;
    cerr << "Reducer R-Tree Build Time: " << totalRTreeBuildTime.count() / 1000.0 << "us"<< "     " << "reducer_id" << taskIdStr << endl;
    //总时间-精炼时间=使用rtree进行mbr的过滤时间
    cerr << "Reducer R-Tree Search Time: " << (totalRTreeSearchTime.count() - totalContainsTime.count()) / 1000.0 << "us"<< "     " << "reducer_id" << taskIdStr << endl;
    cerr << "Reducer Contains Check Time: " << totalContainsTime.count() / 1000.0 << "us"<< "     " << "reducer_id" << taskIdStr << endl;
    cerr << "Reducer Total Reducer Time: " << duration_cast<microseconds>(endTime - startTime).count() / 1000.0 << "us"<< "     " << "reducer_id" << taskIdStr << endl;
    cerr << "Reduce IO Read Time: " << totalIOTime.count() / 1000.0 << "us" << "     " << "reducer_id" << taskIdStr << endl;
    cerr << "Reducer For loop execution time: " << forLoopDuration / 1000.0 << " us" << "     " << "reducer_id" << taskIdStr << endl; 
    cerr << "Reduce push_back Time: " << totalPushBackTime.count() / 1000.0 << "us" << "     " << "reducer_id" << taskIdStr << endl;



    return 0;
}
