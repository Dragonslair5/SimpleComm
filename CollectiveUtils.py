import sys
from tp_utils import *
from MPI_Constants import *
from SendRecv import *
from Col_Bcast_Rank import *







#class CollectiveAlgorithms(Col_Bcast):



#    def __init__(self, nRanks: int, my_rank: int)



class CollectiveUtils:



    @staticmethod
    def layer_my_SendRecvList(sr_list: typing.List[SendRecv]):
        layered_list = [];
        layered_list.append([]);

        for i in range(len(sr_list)):
            col_id = sr_list[i].col_id;
            if len(layered_list) < col_id:
                layered_list.append([])
            layered_list[col_id-1].append(sr_list[i]);
        
        return layered_list;