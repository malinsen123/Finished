//only work for full tree mode
uint_least64_t * uszram_kv_scan(uint_least64_t key_start, uint_least64_t key_end){
	
	uint_least64_t tree_key_start = key_start/RANGE_SPAN; //Find the specific tree leave for this item

    uint_least64_t tree_key_end = key_end/RANGE_SPAN;

	uint_least64_t array_size = tree_key_end - tree_key_start +1;

	int return_keys[array_size];
	void*  returned_pointers[array_size];

	int num_found = findRange(root, tree_key_start, tree_key_end, false, return_keys,returned_pointers);

	//printf("In kv_scan the num_found is %d\n", num_found);

	if(!num_found)
	{
		printf("None found.\n");
		return NULL;
	}

	//page_init decompress_pages[num_found];


	//int decompress_pages =0;

    uint_least64_t * find_keys = malloc(sizeof(uint_least64_t)*(key_end-key_start +1));
    int find_key_nums =0;


	for(int i =0;i<num_found;i++)
	{
		record* record_pointer = (record *)returned_pointers[i];

		struct page* pg = &record_pointer->page_region[0];

		page_init pg_buffer;

		uszram_decompress_page_no_cg(pg, (char *)&pg_buffer);
		apply_cg_to_page(pg, (char *)&pg_buffer);

		for(int j=0; j<BLK_PER_PG; j++)
		{
			blk * tar_blk = &pg_buffer.new_blks[j];

			//printf("The kv_pairs in blks is %d\n",tar_blk->kv_nums);
			for(int k=0; k<tar_blk->kv_nums;k++)
			{
				//printf("THe key in the blk is %lu\n",tar_blk->items[k].key);

				if(tar_blk->items[k].key<key_start || tar_blk->items[k].key>key_end)
					continue;

				find_keys[find_key_nums] = tar_blk->items[k].key;
				find_key_nums +=1;
			}

		}


	}

	//printf("The find_key_nums is %d\n",find_key_nums );

	for(int i =0; i<find_key_nums;i++)
		//printf("The find key is %lu\n", find_keys[i]);


	return find_keys;

}


void *YCSB_E(void * thread_argu)
{

	test_argu * argu=(test_argu*)thread_argu;

	printf("The test_num is %u\n",argu->test_num );


	double * times = malloc(sizeof(double)*argu->test_num);

	struct timespec start, end;
	double duration, total_duration;

	total_duration = 0.0;

	rand_val((int) atoi("10"));

	for(int i=0;i<argu->test_num;i++)
	{
		//printf("The key data is %s\n",key_records[i]);
		//srand(i+argu->id*argu->test_num);
		//uint_least64_t index = rand_int(0,argu->key_num);

		#ifdef UNIFORM
		uint_least64_t index = rand_int(0,argu->key_num);
		#else 
		uint_least64_t index = zipf2(ALPHA, argu->key_num);
		#endif

		uint_least64_t test_key_start = index;
		uint_least64_t test_key_end = index +99;
		//uint_least64_t test_key = hash1(index);

		//printf("The i value is %d and index is %d\n",i,index);
		printf("Try to scan the key start from %lu\n",test_key_start);
		#if DEBUG_MODE
			printf("Try to find the key %lu\n",test_key_start);
		#endif


		timespec_get(&start,TIME_UTC);
		uint_least64_t* ret = uszram_kv_scan(test_key_start,test_key_end);
		timespec_get(&end,TIME_UTC);

		free(ret);
		duration = end.tv_sec - start.tv_sec + (end.tv_nsec - start.tv_nsec) / 1000000000.0;
		times[i] = duration;
		total_duration +=duration;


		if(ret!=NULL)
		{
			
			#if DEBUG_MODE
			printf("The value data is %s\n",ret[0]);
			#endif
		}
	}

	pthread_mutex_lock(&read_lock);
	

	char output_file[60];
	sprintf(output_file,"client%d_YCSB_E",argu->nb_client);
	FILE * outfile = fopen(output_file,"a+");

	for(int i=0;i<argu->test_num;i++)
	{
		fprintf(outfile, "%.10f\n",times[i]);
	}

	
	fclose(outfile);
	free(times);

	pthread_mutex_unlock(&read_lock);

	pthread_exit(NULL);

	printf("Total_time spent for worker %d is %f\n", argu->id, total_duration);

}
