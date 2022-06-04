#include<iostream>
//#include<windows.h>
#include <unistd.h>
#include<mpi.h>
#include<vector>
#include <algorithm>
#include <ctime>
#include <fstream>
#include<string.h>
using namespace std;
#define TOTAL_SIZE 500000
#define task_count 1000 //总任务分为多个小包
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
	int color=0;
	const char* name = "master0";
	if (!abs(strcmp(processor_name, name)))
	{
		color = 0;
	}
	const char* name0 = "master1";
	if (!abs(strcmp(processor_name, name0)))
	{
		color = 1;
	}
	const char* name1 = "node1";
	const char* name2 = "node2";
	if (!abs(strcmp(processor_name, name1)))
	{
		color = 0;
	}
	if (!abs(strcmp(processor_name, name2)))
	{
		color = 0;
	}
	const char* name3 = "node3";
	const char* name4 = "node4";
	if (!abs(strcmp(processor_name, name3)))
	{
		color = 1;
	}
	if (!abs(strcmp(processor_name, name4)))
	{
		color = 1;
	}
	cout<<"color:"<<color<<",world_rank:"<<world_rank<<",name:"<<processor_name<<endl;
	int sub_array[Size + 1];
	vector<int> task;//任务池中任务号
	vector<int> ended_task;//已经完成的任务号
	int task_data[task_count][Size + 1];
	int end_array[Size + 1];//结束数组
	int length = 0;

	MPI_Comm SplitWorld;
	MPI_Comm_split(MPI_COMM_WORLD, color, key, &SplitWorld);
	int row_size;
	int row_rank;
	MPI_Comm_rank(SplitWorld, &row_rank);
	MPI_Comm_size(SplitWorld, &row_size);
	cout<<"color"<<color<<",world_rank"<<world_rank<<",row_rank"<<row_rank<<",name"<<processor_name<<endl;


	if (row_rank == 0)
	{
		for (int i = 0; i < task_count; i++)
		{
			if (i >= row_size)
			{
				task.push_back(i);
			}
			ended_task.push_back(i);
		}
		task.push_back(0);
		for (int i = 0; i < task_count; i++)
		{
			for (int j = 0; j < Size; j++)
			{
				task_data[i][j] = rand() % TOTAL_SIZE;
			}//将一维数组化为二维数组 变成需要的任务池
			task_data[i][Size] = i;//最后一位为任务号
		}
		for (int k = 0; k < Size + 1; k++)
		{
			end_array[k] = -1;
		}
	}

	MPI_Scatter(task_data, Size + 1, MPI_INT, sub_array, Size + 1, MPI_INT, 0, SplitWorld);

	int flag = 999;
	if (row_rank == 0)
	{
		while (!ended_task.empty())
		{
			if (task.empty())
			{
				sleep(3);
				for (int i = 1; i < row_size; i++)
				{
					MPI_Iprobe(i, 0, SplitWorld, &flag, &status);
					if (flag == 1)
					{
						MPI_Recv(sub_array, Size + 1, MPI_INT, MPI_ANY_SOURCE, 0, SplitWorld, &status);
						ended_task.erase(remove(ended_task.begin(), ended_task.end(), sub_array[Size]), ended_task.end());//删除已经完成的任务
						merge__(sorted_array, length, sub_array, Size);
						length = length + Size;
					}
					MPI_Isend(end_array, Size + 1, MPI_INT, i, 20, SplitWorld,&request);
				}
				int m;
				for (int i = 0; i < ended_task.size(); i++)
				{
					m = ended_task[0];
					qsort(task_data[m], Size, sizeof(int), compare);
					merge__(sorted_array, length, task_data[m], Size);
					length = length + Size;
					ended_task.erase(remove(ended_task.begin(), ended_task.end(), task_data[m][Size]), ended_task.end());//删除已经完成的任务
				}
			}
			else
			{
				MPI_Recv(sub_array, Size + 1, MPI_INT, MPI_ANY_SOURCE, 0, SplitWorld, &status);
				ended_task.erase(remove(ended_task.begin(), ended_task.end(), sub_array[Size]), ended_task.end());//删除已经完成的任务
				merge__(sorted_array, length, sub_array, Size);
				length = length + Size;
				int m = task[0];
				MPI_Isend(task_data[m], Size + 1, MPI_INT, status.MPI_SOURCE, 20, SplitWorld,&request);
				task.erase(remove(task.begin(), task.end(), task_data[m][Size]), task.end());//删除已经发送的任务
			}
		}
	}
	if (row_rank != 0)
	{
		while (1) {
			//排序
			qsort(sub_array, Size, sizeof(int), compare);
			MPI_Send(sub_array, Size + 1, MPI_INT, 0, 0, SplitWorld);
			MPI_Recv(sub_array, Size + 1, MPI_INT, 0, 20, SplitWorld, &status);
			if (sub_array[Size] == -1)
				break;

		}
	}
	cout<<endl;
	cout << world_rank << endl;

	if (world_rank != 0&&row_rank==0)
	{
		MPI_Send(sorted_array, length, MPI_INT, 0, 30, MPI_COMM_WORLD);
	}
	if (world_rank == 0)
	{
		int temp[TOTAL_SIZE * 2];
		int num;
		MPI_Probe(MPI_ANY_SOURCE, 30, MPI_COMM_WORLD, &status);
		MPI_Get_count(&status, MPI_INT, &num);
		MPI_Recv(temp, num, MPI_INT, status.MPI_SOURCE, 30, MPI_COMM_WORLD, &status);
		merge__(temp, length, sorted_array, num);
		length = length + num;
		ofstream ofs;
		ofs.open("sorted_array.txt", ios::out);
		for (int i = 0; i < length; i++)
		{
			ofs << temp[i] << " ";
		}
		cout << endl << length;
		cout<<"over";
		MPI_Abort(MPI_COMM_WORLD, 110);
	}

	MPI_Finalize();
}
