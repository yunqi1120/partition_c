#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>
#include <chrono>
#include <cstdlib>
#include "RTree.h"

using namespace std;
using MyRTree = RTree<int, double, 2, double, 8, 4>;
using namespace std::chrono;

int main(int argc, char* argv[]) {
    if (argc != 2) {
        cerr << "Usage: " << argv[0] << " <partitionFilePath>" << endl;
        return 1;
    }

    const char* taskId = getenv("mapreduce_task_id");
    string taskIdStr = taskId ? taskId : "Unknown Task ID";

    auto totalStartTime = high_resolution_clock::now();
    microseconds totalPartitionFileReadTime(0);
    microseconds totalDataReadTime(0);
    microseconds totalSearchTime(0);
    microseconds totalRtreeInsertTime(0); 


    MyRTree rtree;
    ifstream partitionFile(argv[1]); //分区文件
    if (!partitionFile.is_open()) {
        cerr << "Error: Unable to open partition file at: " << argv[1] << endl;
        return 1; 
    }

    string line;
    while (true) {
        auto lineReadStartTime = high_resolution_clock::now();
        if (!getline(partitionFile, line)) break; //文件输入

        istringstream iss(line);
        int partitionIndex;
        double minX, minY, maxX, maxY;
        if (!(iss >> partitionIndex >> minX >> minY >> maxX >> maxY)) {
            cerr << "Invalid line format in partition file: " << line << endl;
            continue;
        }
        auto lineReadEndTime = high_resolution_clock::now();
        //读入分区文件+读入分区坐标
        totalPartitionFileReadTime += duration_cast<microseconds>(lineReadEndTime - lineReadStartTime);

        auto rtreeInsertStartTime = high_resolution_clock::now(); 
        double min[2] = {minX, minY};
        double max[2] = {maxX, maxY};
        rtree.Insert(min, max, partitionIndex); //rtree的建立过程
        auto rtreeInsertEndTime = high_resolution_clock::now(); 
        totalRtreeInsertTime += duration_cast<microseconds>(rtreeInsertEndTime - rtreeInsertStartTime);

    }

    string wktLine;
    while (true) {
        auto dataReadStartTime = high_resolution_clock::now();
        if (!getline(cin, wktLine)) break; //标准输入

        istringstream wktStream(wktLine);
        string id, mbrStr, wkt;
        //存入id、mbrStr、wkt
        if (!(getline(wktStream, id, ',') && getline(wktStream, mbrStr, ',') && getline(wktStream, wkt))) {
            cerr << "Invalid line format in WKT input: " << wktLine << endl;
            continue; 
        }

        istringstream mbrStream(mbrStr);
        double minX, minY, maxX, maxY;
        //mbr坐标读入
        if (!(mbrStream >> minX >> minY >> maxX >> maxY)) {
            cerr << "Invalid MBR format in WKT input: " << mbrStr << endl;
            continue; 
        }
        auto dataReadEndTime = high_resolution_clock::now();
        //标准输入+分隔数据+读入mbr坐标
        totalDataReadTime += duration_cast<microseconds>(dataReadEndTime - dataReadStartTime);

        double min[2] = {minX, minY};
        double max[2] = {maxX, maxY};

        auto searchStartTime = high_resolution_clock::now();
        //在rtree中进行查找
        rtree.Search(min, max, [&id, &wkt](const int& foundId) {
            cout << foundId << "\t" << id << "," << wkt << endl;
            return true;
        });
        auto searchEndTime = high_resolution_clock::now();
        totalSearchTime += duration_cast<microseconds>(searchEndTime - searchStartTime);
    }
    //map总时间
    auto totalEndTime = high_resolution_clock::now();

    //日志形式记录结果
    cerr << "Mapper Partition File Read Time: " << totalPartitionFileReadTime.count() / 1000.0 << "us"<< "     " << "mapper_id" << taskIdStr << endl;
    cerr << "Mapper Original Data Read Time: " << totalDataReadTime.count() / 1000.0 << "us"<< "     " << "mapper_id" << taskIdStr << endl;
    cerr << "Mapper R-Tree Build Time: " << totalRtreeInsertTime.count() / 1000.0 << "us"<< "     " << "mapper_id" << taskIdStr << endl; 
    cerr << "Mapper MBR Search Time: " << totalSearchTime.count() / 1000.0 << "us"<< "     " << "mapper_id" << taskIdStr << endl;
    cerr << "Mapper Total Mapper Time: " << duration_cast<microseconds>(totalEndTime - totalStartTime).count() / 1000.0 << "us"<< "     " << "mapper_id" << taskIdStr << endl;

    return 0;
}
