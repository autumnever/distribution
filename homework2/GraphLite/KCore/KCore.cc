/* 1, 201618013229025, Zhang Qiuping */
/**
 * @file KCore.cc
 * @author  Qiuping Zhang
 * @version 0.1
 *
 * @section LICENSE 
 * 
 * This file implements KCore algorithm using graphlite API.
 *
 */

#include <stdio.h>
#include <string.h>
#include <math.h>
#include <iostream>

#include "GraphLite.h"

#define VERTEX_CLASS_NAME(name) KCore##name

#define EPS 1e-6

//Vertex Value Type, including is_deleted and current_degree
typedef struct VertexType
{
    int is_deleted;
    int current_degree;
}VertexType;

int m_KCore_K;   //the value of K in KCore algorithm

class VERTEX_CLASS_NAME(InputFormatter): public InputFormatter {
public:
    int64_t getVertexNum() {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);   //get the number of vertex in graph
        printf("at class PageRankVertexInputFormatter: m_total_vertex= %lld \n",n);
        m_total_vertex= n;
        return m_total_vertex;
    }
    int64_t getEdgeNum() {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);  //get the number of edge in graph
        printf("at class PageRankVertexInputFormatter: m_total_edge= %lld \n",n);
        m_total_edge= n;
        return m_total_edge;
    }
    int getVertexValueSize() {
        m_n_value_size = sizeof(VertexType);
        return m_n_value_size;
    }
    int getEdgeValueSize() {
        m_e_value_size = sizeof(double);
        return m_e_value_size;
    }
    int getMessageValueSize() {
        m_m_value_size = sizeof(int);
        return m_m_value_size;
    }
    void loadGraph() {
        unsigned long long last_vertex;
        unsigned long long from;
        unsigned long long to;
        double weight = 0;
        
        VertexType value;
        value.is_deleted = 0;   //0 present the vertex is not deleted
        int outdegree = 0;
        
        const char *line= getEdgeLine();

        // Note: modify this if an edge weight is to be read
        //       modify the 'weight' variable

        sscanf(line, "%lld %lld", &from, &to);
        addEdge(from, to, &weight);  //InputFormatter.h

        last_vertex = from;
        ++outdegree;
        //printf("Excute loadGraph(), m_total_edge= %ld \n",m_total_edge);
        for (int64_t i = 1; i < m_total_edge; ++i) {
            line= getEdgeLine();

            // Note: modify this if an edge weight is to be read
            //       modify the 'weight' variable

            sscanf(line, "%lld %lld", &from, &to);
            if (last_vertex != from) {

                value.current_degree = outdegree;
                addVertex(last_vertex, &value, outdegree);
                //printf("vertex: %lld current_degree = %d\n",last_vertex,value.current_degree);
                last_vertex = from;
                outdegree = 1;
            } else {
                ++outdegree;
            }
            addEdge(from, to, &weight);
 
        }

        value.current_degree = outdegree;
        addVertex(last_vertex, &value, outdegree);
        //printf("current_degree = %d\n",value.current_degree);
    }
};

class VERTEX_CLASS_NAME(OutputFormatter): public OutputFormatter {
public:
    void writeResult() {
        int64_t vid;
        VertexType value;
        char s[1024];

        for (ResultIterator r_iter; ! r_iter.done(); r_iter.next() ) {
            r_iter.getIdValue(vid, &value);   //OutputFormatter.h

            if(value.is_deleted == 0){   //print the vid of not deleted vertex
                //int n = sprintf(s, "%lld: %d\n", (unsigned long long)vid, value.current_degree);
                int n = sprintf(s, "%lld\n", (unsigned long long)vid);
                writeNextResLine(s, n);
            }
        }
    }
};


// An aggregator that records a double value tom compute sum
class VERTEX_CLASS_NAME(Aggregator): public Aggregator<double> {
public:
    void init() {
        m_global = 0;
        m_local = 0;
    }
    void* getGlobal() {
        return &m_global;
    }
    void setGlobal(const void* p) {
        m_global = * (double *)p;
    }
    void* getLocal() {
        return &m_local;
    }
    void merge(const void* p) {
        m_global += * (double *)p;
        printf("excute merge() on PageRankAggregator class, m_global= %f \n",m_global);
    }
    void accumulate(const void* p) {
        m_local += * (double *)p;
        printf("excute accumulate() on PageRankAggregator class, m_local= %f \n",m_local);
    }
};


class VERTEX_CLASS_NAME(): public Vertex <VertexType, double, int> {
public:
    void compute(MessageIterator* pmsgs) {
        printf("Excute compute(), MessageIterrator *pmsgs, pmsgs.size= %d\n", pmsgs->m_vector_size);
        VertexType val;
        if (getSuperstep() == 0){
            val.is_deleted = 0;   //set all vertex as not deleted
        }
        else{ 
            val=getValue(); //initialize val

            if (getSuperstep() >= 2) {
                //printf("vertextype is_deleted = %d\n",getValue().is_deleted);
                double global_val = * (double *)getAggrGlobal(0);
                //printf ("global_val = %f\n",global_val);
                if (global_val < EPS || getValue().is_deleted >= 1) { //global_val not change
                    voteToHalt(); return;
                }
            }
          
            for ( ; ! pmsgs->done(); pmsgs->next() ) {
                    val.current_degree = getValue().current_degree - 1;
                    mutableValue()->current_degree = val.current_degree;
            }

            //printf("value = %d, m_KCore_K = %d\n", getValue().current_degree, m_KCore_K);
            
            double tmp_is_deleted = getValue().is_deleted;

            if(getValue().current_degree < m_KCore_K && getValue().is_deleted==0){
                
                val.is_deleted = getValue().is_deleted + 1;  //update is_deleted
                mutableValue()->is_deleted = val.is_deleted;
                sendMessageToAllNeighbors(getValue().is_deleted);    //Vertex.h
            }
            double acc = fabs(getValue().is_deleted - tmp_is_deleted);
            accumulateAggr(0, &acc);  //judge the convergence
        }
        
    }
};

class VERTEX_CLASS_NAME(Graph): public Graph {
public:
    VERTEX_CLASS_NAME(Aggregator)* aggregator;

public:
    // argv[0]: KCore.so
    // argv[1]: <input path>
    // argv[2]: <output path>
    // argv[3]: <K>
    void init(int argc, char* argv[]) {

        setNumHosts(5);  //machine count = 5, one master and four workers
        setHost(0, "localhost", 1411);  //id, hostname, port
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        //if (argc < 3) {
        if (argc < 4) {  //the number of param
           printf ("Usage: %s <input path> <output path>\n", argv[0]);
           exit(1);
        }

        m_pin_path = argv[1];  //input file path
        printf("inpath = %s\n",m_pin_path);
        m_pout_path = argv[2];  //output file path
        printf("outpath = %s\n",m_pout_path);
        m_KCore_K = atoi(argv[3]);  //the degree of KCore
        printf("m_KCore_K = %d\n",m_KCore_K);
        

        aggregator = new VERTEX_CLASS_NAME(Aggregator)[1];
        regNumAggr(1);    //Graph.h
        regAggr(0, &aggregator[0]);  //Graph.h
    }

    void term() {
        delete[] aggregator;
    }
};

/* STOP: do not change the code below. */
extern "C" Graph* create_graph() {
    Graph* pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph* pobject) {
    delete ( VERTEX_CLASS_NAME()* )(pobject->m_pver_base);
    delete ( VERTEX_CLASS_NAME(OutputFormatter)* )(pobject->m_pout_formatter);
    delete ( VERTEX_CLASS_NAME(InputFormatter)* )(pobject->m_pin_formatter);
    delete ( VERTEX_CLASS_NAME(Graph)* )pobject;
}
