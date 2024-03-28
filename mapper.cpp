#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>
#include "RTree.h"

using MyRTree = RTree<int, double, 2, double, 8, 4>;

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <partitionFilePath>" << std::endl;
        return 1;
    }

    MyRTree rtree;

    std::ifstream partitionFile(argv[1]);//分区文件
    if (!partitionFile.is_open()) {
        std::cerr << "Error: Unable to open partition file at: " << argv[1] << std::endl;
        return 1; 
    }

    std::string line;
    while (std::getline(partitionFile, line)) {//文件输入
        std::istringstream iss(line);
        int partitionIndex;
        double minX, minY, maxX, maxY;
        if (!(iss >> partitionIndex >> minX >> minY >> maxX >> maxY)) {//读入mbr坐标
            std::cerr << "Invalid line format in partition file: " << line << std::endl;
            continue;
        }
        double min[2] = {minX, minY};
        double max[2] = {maxX, maxY};
        rtree.Insert(min, max, partitionIndex);//rtree的建立过程
    }

    std::string wktLine;
    while (std::getline(std::cin, wktLine)) {//标准输入
        std::istringstream wktStream(wktLine);
        std::string id, mbrStr, wkt;

        //存入id、mbrStr、wkt
        if (!(std::getline(wktStream, id, ',') && std::getline(wktStream, mbrStr, ',') && std::getline(wktStream, wkt))) {
            std::cerr << "Invalid line format in WKT input: " << wktLine << std::endl;
            continue; 
        }

        std::istringstream mbrStream(mbrStr);
        double minX, minY, maxX, maxY;

        if (!(mbrStream >> minX >> minY >> maxX >> maxY)) {
            std::cerr << "Invalid MBR format in WKT input: " << mbrStr << std::endl;
            continue; 
        }

        double min[2] = {minX, minY};
        double max[2] = {maxX, maxY};

        //在rtree中进行查找
        rtree.Search(min, max, [&id, &wkt](const int& foundId) {
            std::cout << foundId << "\t" << id << "," << wkt << std::endl;
            return true; 
        });
    }

    return 0;
}
