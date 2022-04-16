#include<iostream>
//#include<windows.h>
#include<mpi.h>
#include<vector>
#include <algorithm>
#include <fstream>

using namespace std;
#define TOTAL_SIZE 500000
#define task_count 100 //总任务分为10个小包
#define Size TOTAL_SIZE/task_count  //每个小包的size
int compare(const void* a, const void* b)
{
	return *(int*)a - *(int*)b;
}
void merge__(int* nums1, int m, int* nums2, int n) {
	int p = m - 1;      // p 指向 nums1[m - 1]
	int q = n - 1;      // q 指向 nums2[n - 1]
	int k = m + n - 1;  // k 指向 nums1[m + n - 1]
	while (p >= 0 && q >= 0) {
		nums1[k--] = nums1[p] > nums2[q] ? nums1[p--] : nums2[q--];
	}

	/* 若 n > m，nums1 遍历完成，将 nums2 中尚未遍历完的元素拷贝到 nums1 中 */
	while (q >= 0) {
		nums1[k--] = nums2[q--];
	}
}
int main(int argc, char* argv[])
{
	int RankID;
	srand(time(NULL));
	int world_rank;
	int process_num;
	int sorted_array[TOTAL_SIZE];
	//int original_array[TOTAL_SIZE];
	MPI_Status status;
	MPI_Request request;
	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &process_num);
	int namelen;
	char processor_name[MPI_MAX_PROCESSOR_NAME];//设备名
	MPI_Get_processor_name(processor_name, &namelen);

	int key = world_rank;//在通信域中排序用的
	int color = 0;
	if (processor_name == "master0")
	{
		color = 0;
	}
	if (processor_name == "master1")
	{
		color = 1;
	}
	if (processor_name == "node1" || processor_name == "node2")
	{
		color = 0;
	}
	if (processor_name == "node3" || processor_name == "node4")
	{
		color = 1;
	}
	int sub_array[Size + 1];
	vector<int> task;//任务池中任务号
	vector<int> ended_task;//已经完成的任务号
	int task_data[task_count][Size + 1];
	int end_array[Size + 1];//结束数组
	int length = 0;


	MPI_Comm SplitWorld;
	MPI_Comm_split(MPI_COMM_WORLD, world_rank % 2, key, &SplitWorld);

	int row_size;
	int row_rank;
	MPI_Comm_rank(SplitWorld, &row_rank);
	MPI_Comm_size(SplitWorld, &row_size);

	//int test[39] = { 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36 ,37,38,39};
	if (row_rank == 0)
	{
		for (int i = 0; i < task_count; i++)
		{
			if (i >= row_size)
				task.push_back(i);
			ended_task.push_back(i);
		}
		task.push_back(0);
		//cout << "原始数据：" << endl;
		for (int i = 0; i < task_count; i++)
		{
			for (int j = 0; j < Size; j++)
			{
				//original_array[i* Size+j] = rand() % TOTAL_SIZE;
				task_data[i][j] = rand() % TOTAL_SIZE;
				//cout << task_data[i][j] << " ";
			}//将一维数组化为二维数组 变成需要的任务池
			task_data[i][Size] = i;//最后一位为任务号
			//original_array[i * Size + Size] = i;
			//cout << task_data[i][Size];
			//cout << endl;
		}
		//cout << "**********************************************" << endl;
		for (int k = 0; k < Size + 1; k++)
		{
			end_array[k] = -1;
		}
	}
	//cout << row_rank << ' ' << world_rank << endl;
	MPI_Scatter(task_data, Size + 1, MPI_INT, sub_array, Size + 1, MPI_INT, 0, SplitWorld);


	if (row_rank == 0)
	{
		while (!ended_task.empty())
		{
			MPI_Recv(sub_array, Size + 1, MPI_INT, MPI_ANY_SOURCE, 0, SplitWorld, &status);
			//auto iter = std::remove(ended_task.begin(), ended_task.end(), sub_array[Size]);
			ended_task.erase(remove(ended_task.begin(), ended_task.end(), sub_array[Size]), ended_task.end());//删除已经完成的任务
			merge__(sorted_array, length, sub_array, Size);
			length = length + Size;
			//for (int temp = 0; temp <= Size; temp++)
			//{
			//	//cout << sub_array[temp] << " ";
			//}

			//cout << "source " << status.MPI_SOURCE << " ";
			if (!task.empty())
			{
				int m = task[0];
				MPI_Send(task_data[m], Size + 1, MPI_INT, status.MPI_SOURCE, 20, SplitWorld);
				//iter = std::remove(task.begin(), task.end(), task_data[m][Size]);
				task.erase(remove(task.begin(), task.end(), task_data[m][Size]), task.end());//删除已经发送的任务
			}
			else
			{
				MPI_Send(end_array, Size + 1, MPI_INT, status.MPI_SOURCE, 20, SplitWorld);
			}
		}
	}
	if (row_rank != 0)
	{
		while (1) {
			/*for (int k = 0; k < Size + 1; k++)
				cout << sub_array[k] << " ";
			cout << "world " << world_rank << " row " << row_rank << endl;*/

			//paixu
			qsort(sub_array, Size, sizeof(int), compare);
			MPI_Send(sub_array, Size + 1, MPI_INT, 0, 0, SplitWorld);
			MPI_Recv(sub_array, Size + 1, MPI_INT, 0, 20, SplitWorld, &status);
			if (sub_array[Size] == -1)
				break;
			//for (int k = 0; k < Size + 1; k++)
			//	cout << sub_array[k] << " ";
			//cout << world_rank << endl;
		}
	}
	//if (row_rank == 0)
	//{

	//	for (int temp = 0; temp < length; temp++)
	//	{
	//		cout << sorted_array[temp] << " ";
	//	}
	//	cout << endl << length;
	//	cout << world_rank;
	//}

	if (world_rank == 1)
	{

		MPI_Send(sorted_array, TOTAL_SIZE, MPI_INT, 0, 30, MPI_COMM_WORLD);
	}
	if (world_rank == 0)
	{
		ofstream ofs;
		ofs.open("sorted",ios::out);
		int temp[TOTAL_SIZE * 2];
		MPI_Recv(temp, TOTAL_SIZE, MPI_INT,1, 30, MPI_COMM_WORLD, &status);
		merge__(temp, length, sorted_array, length);
		length = length * 2;
		for (int i = 0; i < length; i++)
		{
			ofs << temp[i] << " ";
		}
		cout << endl << length;
	}
	MPI_Finalize();
}
