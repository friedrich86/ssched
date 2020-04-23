/*
ssched app
-------------------------------------------------
支持cron表达式
支持安卓手机闹钟中的 法定工作日(智能跳过节假日)
-------------------------------------------------
杨晓君
*/

#include <stdio.h>
#include <stddef.h>
#include <time.h>
#include <assert.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>

/*
ti osa头文件
*/
#include "osa_tsk.h"

/*
mqtt头文件
*/
#include <mosquitto.h>
#include <mqtt_protocol.h>

/*
json-c头文件
*/
#include "json.h"

/*
github开源链表操作库头文件
*/
#include "list.h"

/*
cron表达式解析模块
*/
#include "ccronexpr.h"

/*
边缘计算网关中最多存储2000个假日；
*/
//TODO
//可以动态加载，比当前时间早的假日可以不用加载，这样2000这个数字可以减小；
#define ECG_MAX_HOLIDAYS_NUM   (2000)

//支持最多的调度个数，一个调度对应一个CRON表达式；
#define ECG_MAX_CRON_EXP_NUM   (1000)

//单个调度最多进行20个挂接任务
//TODO:暂时用不到这个?
#define ECG_MAX_SCHE_TASK      (20)


#define SSCHED_BROKER_HOST     "localhost"
#define SSCHED_BROKER_PORT     1883

#define DATE_FORMAT "%Y-%m-%d_%H:%M:%S"

/*
ssched app调试使能，如果禁止使能，直接注释掉；
*/
#define SSCHED_DEBUG_EN        (1)


/*
单个时间周期，按照60秒计算；
*/
#define SSCHED_TIME_SLICE_IN_SEC   (60)

/*
单个时间计划任务调度，
包含时间规则,cron格式；
到达对应时间点要执行的事情，json对象；
*/
typedef struct ssches
{
	//cron表达式形式的日期时间
	char * pattern;

	/*
	代表是否是法定工作日(智能跳过节假日)
	*/
	bool is_woringdays;

	//到达对应的时间点要执行的任务；
	/*
	单个调度任务列表最多有24个可执行任务
	*/
	struct json_object * ssched_tasks[24];

	/*
	实际执行任务个数；
	*/
	unsigned char tasks_num;
}ecg_ssches;


/*
智能调度数据结构链表头，双向链表；
*/
list_t * my_ssched_list;


/*数据结构，包含最近的节假日，支持开机时静态从持久化存储中加载，或者动态加载*/
struct tm my_holidas[ECG_MAX_HOLIDAYS_NUM];


//智能时间调度APP 监听MQTT线程handle
OSA_ThrHndl ssched_li_thr_handle;

//智能时间调度APP 调度线程handle
OSA_ThrHndl ssched_sche_thr_handle;

/*
用于连接内部broker的mqtt数据结构；
*/
struct mosquitto * ssched_mosq = NULL;


/*
下一个要被执行的计划任务的链表索引
*/
Uint8 my_mo_ur_task_index;

/*
整个ssched app最近的任务的时间调度点
初始化赋值最大数值

记录下一个要执行的任务的时间点，从1970年开始的秒数?
*/
time_t my_mo_ur_task_tp = 0x7fffffff;

/*
代表我们的 时间调度表 是否被修改了；
默认没有被修改；
*/
bool is_our_sche_mod = false;


/*
本地函数，添加 时间计划 调度

入口参数：
_p_json_obj  对应新增的json数据对象

出口参数：

*/
static void _ssched_add_sche(const struct json_object * _p_json_obj)
{
	/*
	cmd对应的json对象
	*/
	//struct json_object * p_cmd_obj;
	/*
	pattern对应的json对象
	*/
	struct json_object * p_pattern_obj;

	/*
	workingDays对应的json对象
	*/
	struct json_object * p_wdays_obj;
	bool is_wdays;

	/*
	cron表达式
	*/
	const char * p_cron_pattern_str;

	/*
	actionList执行任务清单对应json对象
	*/
	struct json_object * p_actlist_obj;

	/*
	整个这个schedule调度的json
	*/
	struct json_object * p_jobj = (struct json_object *)(_p_json_obj);


	/*
	执行任务个数；
	*/
	Uint8 act_num;

	Uint8 i;


	#ifdef SSCHED_DEBUG_EN
		printf("ssched app: enter _ssched_add_sche.\n");
	#endif

	/*
	动态分配内存块；
	*/
	ecg_ssches * my_single_sche = (ecg_ssches *)malloc(sizeof(ecg_ssches));
	/*
	加入链表
	*/
	list_node_t * my_single_node = list_node_new(my_single_sche);

	/*
	获取pattern的json对象
	*/
	if(json_object_object_get_ex(p_jobj, "pattern", &p_pattern_obj))
	{
		/*
		将数据动态插入数据结构中；
		*/
		/*
		解析出cron的字符串；
		*/
		p_cron_pattern_str = json_object_get_string(p_pattern_obj);

		#ifdef SSCHED_DEBUG_EN
			printf("cron pattern is %s.\n", p_cron_pattern_str);
		#endif

		/*
		cron表达式字符串复制；
		*/
		my_single_sche->pattern = (char *)(p_cron_pattern_str);
	}
	else
	{
		#ifdef SSCHED_DEBUG_EN
			printf("ssched app: NO pattern json obj in addSchedule message.\n");
		#endif
	}

	/*
	抓取是否是法定工作日；
	*/
	if (json_object_object_get_ex(p_jobj, "workingDays", &p_wdays_obj))
	{
		is_wdays = json_object_get_boolean(p_wdays_obj);

		#ifdef SSCHED_DEBUG_EN
			printf("%s.\n", is_wdays?"isWorkingDays":"isNOTworkdingDays");
		#endif

		my_single_sche->is_woringdays = is_wdays;
	}

	/*
	抓取解析动作列表
	*/
	if(json_object_object_get_ex(p_jobj, "actionLists", &p_actlist_obj))
	{
		act_num = json_object_array_length(p_actlist_obj);
		#ifdef SSCHED_DEBUG_EN
			printf("ssched app : addSchedule for %s.\n", is_wdays?"isWorkingDays":"isNOTworkdingDays");
			printf("ssched app : act_num is %d.\n", act_num);
		#endif

		/*
		TODO:如果任务数量超过24个?首先要在用户界面做限制;
		*/	

		for(i = 0; i < act_num; i++)
		{
			/*
			获取执行任务清单中的json对象,复制到目标对象
			*/
			my_single_sche->ssched_tasks[i] = json_object_array_get_idx(p_actlist_obj, i);
		}

		/*
		更新要执行的任务个数
		*/
		my_single_sche->tasks_num = act_num;
	}

	/*
	将节点插入链表中，即时加入当前的数据结构中；
	*/
	list_rpush(my_ssched_list, my_single_node);

	#ifdef SSCHED_DEBUG_EN
		printf("ssched app : insert new scheduled task to list.\n");
	#endif

	/*
	TODO:
	通知调度线程?当前插入新的结构?其实也不用通知，每次搜索都遍历链表
	*/
}

/*
解析mqtt发送过来的json消息，并且将其解析到对应数据结构中去；
*/
void process_sche_message(const char * sche_message)
{
	struct json_object * p_jobj;
	struct json_object * p_jobj_string;

	const char * str;

	p_jobj = json_tokener_parse(sche_message);


	#ifdef SSCHED_DEBUG_EN
		printf("ssched app: we get message.\n");
	#endif

	/*
	入口监测，可能字符串不符合json格式；
	直接退出；
	*/
	if (p_jobj == NULL)
	{
		#ifdef SSCHED_DEBUG_EN
			printf("ssched app: subsribed input string is NOT json format.\n");
		#endif
		
		return;
	}

	if(json_object_object_get_ex(p_jobj, "cmd", &p_jobj_string))
	{
		str = json_object_get_string(p_jobj_string);

		printf("str is %s\n", str);

		if(strcmp(str, "addSchedule") == 0)
		{
			/*
			是addSchedule，做两件事情：
			将其进行本地化存储，同时，加入当前数据结构中；
			*/
			/*
			获取 时间计划 编号，保存为相关文件；
			*/
			_ssched_add_sche(p_jobj);	

			/*
			标记时间计划表进行了修改；
			*/
			is_our_sche_mod = true;
		}

		if(strcmp(str, "delSchedule") == 0)
		{
			/*
			如果是delSchedule，就删除本地化存储的文件，同时，删除对应数据结构；
			也可以重新加载，重新加载会简单？
			*/

			/*
			标记时间计划表进行了修改；
			*/
			is_our_sche_mod = true;

			/*
			TODO:已经被排到即将执行的任务，如果要被删除，就要被剔除掉!!!
			*/
		}

		if(strcmp(str, "addHolidays") == 0)
		{
			/*

			*/

			/*
			标记时间计划表进行了修改；
			*/
			is_our_sche_mod = true;
		}

	}
	else
	{
		printf("no cmd key!\n");

	}
	
}

/*
MQTT订阅到主题的回调函数
*/
void ssched_subs_callback(struct mosquitto *mosq, void *obj, int mid, int qos_count, const int *granted_qos)
{
	mosq = mosq;
	obj = obj;
	mid = mid;
	qos_count = qos_count;
	granted_qos = granted_qos;
    printf("ssched app subscribed internal broker.\n");
}

/*
连接上broker的回调函数；
*/
void ssched_con_callback(struct mosquitto *mosq, void *obj, int result, int flags, const mosquitto_property *properties)
{
	if(!result){
		/*
		当连接broker成功的时候，进行主题订阅；
		*/
		//TODO:主题中本机的gatewayDK需要动态获取；
		mosquitto_subscribe(mosq, NULL, "local/gateway/12345678/command", 0);
		
	}else{
		if(result){
				printf("Connection error: %s\n", mosquitto_connack_string(result));
		}
		mosquitto_disconnect_v5(mosq, 0, NULL);
	}
}

/*
收到订阅消息的回调函数；
*/
void ssched_mes_callback(struct mosquitto *mosq, void *obj, const struct mosquitto_message *message, const mosquitto_property *properties)
{
	bool res;

	//UNUSED(obj);
	//UNUSED(properties);


//	if(cfg.remove_retained && message->retain){
//		mosquitto_publish(mosq, &last_mid, message->topic, 0, NULL, 1, true);
//	}

	/*
	根据收到消息的类型，进行操作，然后回复该消息；
	*/

	mosquitto_topic_matches_sub("local/gateway/+/command", message->topic, &res);
	//如果订阅到的不是目标主题，就退出，这次主题数据和我们无关；
	if(res)
	{

	}
	else
	{
		return;
	}


	/*
	将接收到的json格式字符串转换为json对象
	*/

	process_sche_message(message->payload);
}

/*
函数：
找到最近时间点的任务；
get closest point in time task 

输入参数：
无

输出参数：

*/
static void get_clp_task(void)
{
	/*
	cron表达式
	*/
	cron_expr parsed;
	/*
	存放错误标记
	*/
	const char* err = NULL;


	ecg_ssches * _my_sche;

	/*
	当前时间点
	*/
	time_t cur_time;
	/*
	下一次要调度的时间点
	*/
	time_t next_time;
	/*
	下一次最近的调度时间
	*/
	time_t next_time_mo;
	struct tm * my_tm;

	time_t dateinit;

	Uint8 i;

	/*
	TODO:从本地持久化存储中解析json数据，加入到运行时数据结构中；
	*/
	i = 0;

	list_node_t * my_node_sche;

	printf("ssched app: enter get_clp_task.\n");

	/*
	获取当前时间
	*/
	cur_time = time(NULL);
	/*
	my_tm是否需要释放?
	*/
	my_tm = localtime(&cur_time);

	/*
	调试打印
	*/
	#ifdef SSCHED_DEBUG_EN
	printf("now time is \n");

	printf("%d/%d/%d\n",my_tm->tm_mon+1,my_tm->tm_mday,my_tm->tm_year+1900);  
    printf("%d:%d:%d\n",my_tm->tm_hour,my_tm->tm_min,my_tm->tm_sec); 
	#endif

    /*
	最开始，将 下次最近时间调度点 赋值为一个最远时间点；
    */
	next_time_mo = 0x7fffffff;

	while (1)
	{
		/*
		找到离当前时间点最近的任务
		*/

		/*
		从数据结构链表中找到一个个调度任务数据结构
		*/
		my_node_sche = list_at(my_ssched_list, i);

		/*
		第一个链表成员如果为空，代表没有调度任务，直接退出该函数；
		*/
		if ((my_node_sche == NULL) && (i == 0))
		{
			#ifdef SSCHED_DEBUG_EN
				printf("ssched app: no Avaiable schedule.\n");
			#endif

			/*
			没有需要被添加schedule的计划表
			直接退出
			*/	
			return;
		}

		#ifdef SSCHED_DEBUG_EN
			printf("ssched app : ready to get schedule data.\n");
		#endif
		/*
		先拿出数据
		*/
		if (my_node_sche->val != NULL)
		{
			_my_sche = (ecg_ssches *)my_node_sche->val;
		}
		else
		{
			/*
			如果指针为空，直接调到下一个链表结构中去查看表达式；
			*/
			#ifdef SSCHED_DEBUG_EN
				printf("ssched app : 111111.\n");
			#endif
			continue;
		}

		if(_my_sche->pattern != NULL)
		{
			/*
			如果含有pattern cron表达式，就解析；
			*/
			/*
			TODO:
			err如果出错，比如cron表达式错误，就不能进行后续操作，直接退出，否则会出现段错误!
			*/
			cron_parse_expr(_my_sche->pattern, &parsed, &err);
		}
		else
		{
			#ifdef SSCHED_DEBUG_EN
				printf("ssched app : 22222222.\n");
			#endif
			continue;
		}

		/*
		获取下一次的调度时间；
		*/
		
		dateinit = mktime(my_tm);
		next_time = cron_next(&parsed, dateinit);

		/*
		TODO:因为添加了 法定工作日 的支持，所以 需要当前当天是否是 法定工作日
		并根据情况判断，如果不符合要求，就直接进入下一个循环；
		*/

		#ifdef SSCHED_DEBUG_EN
		printf("next time is %d, next_time_mo is %d.\n", next_time, next_time_mo);
		#endif

		/*
		如果本次调度时间点，比最近的时间点还近，就更新next_time_mo
		*/
		if(next_time < next_time_mo)
		{
			/*
			最紧急的时间点在这里被记录
			*/
			next_time_mo = next_time;
			/*
			对应的链表索引在这里被记录
			*/
			my_mo_ur_task_index = i;
		}
		else
		{
			//do nothing
		}


		i++;

		/*
		如果链表中最后一个成员就是当前的成员，也就代表链表已经遍历完毕，可以退出循环；
		*/
		if (list_at(my_ssched_list, -1) == my_node_sche)
		{
			/*
			链表已经遍历一遍了，直接跳出；
			*/
			break;
		}
	}

	/*
	将最紧急任务需要延时的时间点，赋值给相关全局数据结构
	*/
	my_mo_ur_task_tp = next_time_mo;
	
	/*
	调试打印输出，输出下一次调度的时间点
	*/
	#ifdef SSCHED_DEBUG_EN
	struct tm* calnext = localtime(&next_time_mo);

	printf("calnext is :\n");
	printf("%d/%d/%d\n",calnext->tm_mon+1,calnext->tm_mday,calnext->tm_year+1900);  
    printf("%d:%d:%d\n",calnext->tm_hour,calnext->tm_min,calnext->tm_sec); 

	char* buffer = (char*) malloc(21);
    memset(buffer, 0, 21);
    strftime(buffer, 20, DATE_FORMAT, calnext);
    printf("calculated next time is %s.\n", buffer);

    printf("ssched app : most urgent task index is %d.\n", i);

	#endif

	return;
}

/*
实际执行 调度任务 线程

*/
static void ssched_sche_thread(void)
{
	Uint8 _act_num;
	Uint8 i;
	list_node_t * _my_node_now;
	ecg_ssches * _my_sche_now;

	char * _my_task_now;

	/*
	暂存设备deviceKey的json对象
	*/
	struct json_object * my_devK_jobj;
	
	/*
	暂存设备的deviceKey
	*/
	char * _my_devK;

	/*
	要被发布设备的主题字符串
	*/
	char my_mos_topic[64];

	/*
	要被执行的json转换的字符串长度
	*/
	int _my_task_pll;

	/*
	整个变量记录，要睡眠到下一个最紧急任务,app对应线程需要睡眠的秒数
	初始化，无限休眠
	*/
	time_t _my_mo_ur_task_sleep_count = my_mo_ur_task_tp;

	/*
	调试打印：进入线程，打印
	*/
	#ifdef SSCHED_DEBUG_EN
	printf("ssched app: enter ssched_sche_thread\n");
	#endif

	while (1)
	{
		/*
		如果待运行时间大于一分钟，就先休眠一分钟；
		*/
		if (_my_mo_ur_task_sleep_count > SSCHED_TIME_SLICE_IN_SEC)
		{

			#ifdef SSCHED_DEBUG_EN
			printf("ssched app : sleep 60s.\n");
			#endif

			sleep (SSCHED_TIME_SLICE_IN_SEC);


			/*
			TODO:判断是否有时间计划表的变更
			如果没有，就 -60，然后继续演示；
			如果有变更，就重新加载；
			*/

			if (is_our_sche_mod == true)
			{
				get_clp_task();
				/*
				TODO: is_our_sche_mod是否需要加全局保护?
				是否存在互斥?注意！！
				*/

				/*
				在判断时间计划表被修改后，重新运算，排序要被执行的任务，
				完成该操作后，认为时间计划表没有被修改，因为是最新的；
				*/
				is_our_sche_mod = false;

				/*
				因为get_clp_task重新运算了 定时任务，_my_mo_ur_task_sleep_count也要被刷新；
				*/	
				_my_mo_ur_task_sleep_count = my_mo_ur_task_tp - time(NULL);

				#ifdef SSCHED_DEBUG_EN
					printf("ssched app : after get_clp_task, we ready to sleep %d secs.\n", _my_mo_ur_task_sleep_count);
				#endif
			}
			else
			{
				/*
				采用的方式是不断和当前时间做对比，防止当前时间发生调整；
				*/
				_my_mo_ur_task_sleep_count = my_mo_ur_task_tp - time(NULL);
				//_my_mo_ur_task_sleep_count -= SSCHED_TIME_SLICE_IN_SEC;

				#ifdef SSCHED_DEBUG_EN
					printf("ssched app : we ready to sleep %d secs.\n", _my_mo_ur_task_sleep_count);
				#endif
			}
		}
		else
		{
			#ifdef SSCHED_DEBUG_EN
			printf("ssched app : sleep %ds.\n", (int)(_my_mo_ur_task_sleep_count));
			#endif
			/*
			剩余休眠时间
			*/
			sleep (_my_mo_ur_task_sleep_count);

			/*
			到了这里，代表休眠到时间，清零；
			*/
			_my_mo_ur_task_sleep_count -= _my_mo_ur_task_sleep_count;

			/*
			已经休眠到对应时间点，开始执行任务
			*/

			#ifdef SSCHED_DEBUG_EN
			printf("ssched app: time is NOW! do it!run scheduled task!\n");
			#endif

			/*
			实际执行的任务就是把actionLists中信息发布出去；
			*/
			/*
			找到链表中要被执行的数据结构;
			*/

			_my_node_now = list_at(my_ssched_list, my_mo_ur_task_index);
			
			/*
			从链表成员中找到挂接的数据结构
			*/
			_my_sche_now = (ecg_ssches *)_my_node_now->val;
			/*
			获取这次时间计划任务挂的任务个数
			*/
			_act_num = _my_sche_now->tasks_num;

			#ifdef SSCHED_DEBUG_EN
				printf("ssched app : to be scheduled task num is %d.\n", _act_num);
			#endif	

			/*
			依次发布需要发布的消息；
			*/
			for (i = 0; i < _act_num; i++)
			{
				/*
				将json转化为mqtt可以发布的字符串
				*/
				_my_task_now = json_object_get_string(_my_sche_now->ssched_tasks[i]);

				/*
				获取要被调度设备的deviceKey
				*/
				/*
				获取对应的子json object
				*/
				if(json_object_object_get_ex(_my_sche_now->ssched_tasks[i], "deviceKey", &my_devK_jobj))
				{
					/*
					解析出deviceKey
					*/
					_my_devK = json_object_get_string(my_devK_jobj);

					#ifdef SSCHED_DEBUG_EN
						printf("ssched app : to be scheduled devK is %s.\n", _my_devK);
					#endif
				}
				else
				{
					/*
					TODO:如果解析不到deviceKey，怎么处理?
					应该在入口，就是mqtt接收到数据时，就做统一化处理!排除错误处理。
					*/
				}

				/*
				拼出需要发布的主题
				*/
				strcpy(my_mos_topic, "local/gateway/");
				strcat(my_mos_topic, _my_devK);
				strcat(my_mos_topic, "/command");

				#ifdef SSCHED_DEBUG_EN
					printf("ssched app : to be scheduled related topic is %s.\n", my_mos_topic);	
				#endif


				/*
				获取字符串长度，这是整个消息体长度
				*/
				_my_task_pll = strlen(_my_task_now);
				/*
				任务不是空的
				*/

				/*
				实际进行mqtt消息发送
				*/
				mosquitto_publish(ssched_mosq, NULL, (const char *)(my_mos_topic), 
					_my_task_pll, _my_task_now, 0, 0);

			}

			/*
			当次任务执行完毕，都要重新计算 最紧急任务
			函数计算完成后，会重新刷新 my_mo_ur_task_tp 全局变量；
			*/
			get_clp_task();

			/*
			如果当前 时间任务 执行完成，就重新加载my_mo_ur_task_tp

			my_mo_ur_task_tp也在get_clp_task函数中被重新刷新；
			*/
			if (_my_mo_ur_task_sleep_count == 0)
			{
				_my_mo_ur_task_sleep_count = my_mo_ur_task_tp - time(NULL);
			}
		}
	}
}

/*
线程说明：
1.监听MQTT主题；
2.将涉及智能时间调度的消息，加入运行时数据结构或者进行持久化存储；
*/
static void ssched_li_thread(void)
{
    printf("ssched app: enter ssched_li_thread.\n");
/*
初始化mosquitto库
*/
	mosquitto_lib_init();

/*
新建一个mqtt对象
*/
	ssched_mosq = mosquitto_new(NULL, TRUE, NULL);

	if(!ssched_mosq)
	{
		/*
		MQTT初始化失败，打印报错，写入日志，直接退出；
		*/
        printf("CRITICAL ERROR! \n");
        system("logger in ssched app,mosquitto_new failed!");
        return;
	}
	
	/*
	设置订阅回调函数
	*/
	mosquitto_subscribe_callback_set(ssched_mosq, ssched_subs_callback);
	/*
	设置连接回调函数
	*/
	mosquitto_connect_v5_callback_set(ssched_mosq, ssched_con_callback);
	/*
	设置收到消息回调函数
	*/
	mosquitto_message_v5_callback_set(ssched_mosq, ssched_mes_callback);

	/*
	进行实际连接，连接到内部broker;
	*/
	if (mosquitto_connect(ssched_mosq, SSCHED_BROKER_HOST, SSCHED_BROKER_PORT, 60) == MOSQ_ERR_SUCCESS)
	{
		printf("ssched app : connect broker done.\n");
	}
	else
	{
		printf("ssched app : connect broker failed!\n");
	}

	mosquitto_loop_forever(ssched_mosq, -1, 1);

	mosquitto_destroy(ssched_mosq);
	mosquitto_lib_cleanup();

}

int main(void)
{
	#ifdef SSCHED_DEBUG_EN
		printf("ssched app : RUN!\n");
	#endif

	/*
	智能调度链表初始化
	*/
	my_ssched_list = list_new();

	/*
	创建线程，监听MQTT发过来的主题；
	*/
	OSA_thrCreate( &(ssched_li_thr_handle), ssched_li_thread, 2, 0, NULL );

	/*
	创建线程，进行实际的任务调度
	*/
	OSA_thrCreate( &(ssched_sche_thr_handle), ssched_sche_thread, 2, 0, NULL );

	while (1)
	{
		sleep (1);
	}

	return 0;
}


