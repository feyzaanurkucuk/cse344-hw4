#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <pthread.h>
#include <signal.h>

#define EOF_MARKER NULL
char *fileName;
char **lineBuffer;
int num_workers;
int buffer_size;
int lineCount = 0,nextLine = 0;
char *search_term;
int *worker_matches;

int total_matches=0;

volatile sig_atomic_t stop = 0;


pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t not_full = PTHREAD_COND_INITIALIZER;
pthread_cond_t not_empty = PTHREAD_COND_INITIALIZER;
pthread_barrier_t barrier;
// Handle SIGINT (Ctrl+C) gracefully
void sigint_handler(int signum) {
    printf("\nReceived SIGINT signal. Cleaning up...\n");
    stop=1;
    pthread_cond_broadcast(&not_full);
    pthread_cond_broadcast(&not_empty);
}

void *th_manager(void *arg) {
    int fd = open(fileName, O_RDONLY);
    if (fd < 0) {
        perror("open");
        pthread_exit(NULL);
    }

    char ch;
    char line[1024];
    int linePos = 0;

    while (read(fd, &ch, 1) > 0) {
        if (ch == '\n' || linePos == sizeof(line) - 1) {
            line[linePos] = '\0';

            pthread_mutex_lock(&mutex);
            while ((lineCount - nextLine) >= buffer_size && !stop) {
                pthread_cond_wait(&not_full, &mutex);
            }
            if (stop) {
                pthread_mutex_unlock(&mutex);
                close(fd);
                return NULL;
            }


            lineBuffer[lineCount % buffer_size] = strdup(line);
            lineCount++;
            //printf("line read: %s\n",line);
            pthread_cond_signal(&not_empty);
            pthread_mutex_unlock(&mutex);

            linePos = 0;
        } else {
            line[linePos++] = ch;
        }
        if (stop) {
		    pthread_mutex_unlock(&mutex);
		    close(fd);
		    return NULL;
		}    
		}

    // Handle last line 
    if (linePos > 0) {
        line[linePos] = '\0';
        pthread_mutex_lock(&mutex);
        while ((lineCount - nextLine) >= buffer_size) {
            pthread_cond_wait(&not_full, &mutex);
        }
        lineBuffer[lineCount % buffer_size] = strdup(line);
        lineCount++;
        pthread_cond_signal(&not_empty);
        pthread_mutex_unlock(&mutex);
    }

    close(fd);

    // Send EOF marker (NULL) once per worker
    for (int i = 0; i < num_workers; ++i) {
        pthread_mutex_lock(&mutex);
        while ((lineCount - nextLine) >= buffer_size) {
            pthread_cond_wait(&not_full, &mutex);
        }
        lineBuffer[lineCount % buffer_size] = EOF_MARKER;  
        lineCount++;
        pthread_cond_signal(&not_empty);
        pthread_mutex_unlock(&mutex);
    }

    return NULL;
}


void *th_worker(void *arg) {
    int thread_id = *(int *)arg;
    
    free(arg); 
    int match_count = 0;

    while (1) {
        char *line = NULL;
        int index;

        pthread_mutex_lock(&mutex);
        while (lineCount <= nextLine && !stop) {
            pthread_cond_wait(&not_empty, &mutex);
        }
        if (stop) {
            pthread_mutex_unlock(&mutex);
            break;
        }
        index = nextLine;
        line = lineBuffer[index % buffer_size];
        lineBuffer[index % buffer_size] = NULL;
        nextLine++;

        pthread_cond_signal(&not_full);
        pthread_mutex_unlock(&mutex);

        // Check for EOF marker
        if (line == EOF_MARKER) {
            //printf("end of file\n");
            break;
        }

        // Count keyword occurrences
        char *ptr = line;
        int local_matches = 0;
        while ((ptr = strstr(ptr, search_term)) != NULL) {
            local_matches++;
            ptr += strlen(search_term);
        }
          //printf("line in worker %d: %s\n",thread_id,line);
        if (local_matches > 0) {
            printf("[Worker %d] Keyword found %d time(s) in line %d: %s\n", thread_id, local_matches, index + 1, line);
            match_count += local_matches;
        }


        free(line);
    }
    pthread_mutex_lock(&mutex);
    worker_matches[thread_id-1] += match_count;
    pthread_mutex_unlock(&mutex);
    pthread_barrier_wait(&barrier);
   
    return NULL;
}

void *th_final_report(void *arg){
	pthread_barrier_wait(&barrier);

	if(stop){
		return NULL;
	}
	printf("\nSummary report:\n");
	for(int i=0 ; i<num_workers; i++){
		printf("Worker %d found %d match(es)\n",i+1,worker_matches[i]);
	}

	int total_matches=0;
	for(int i=0; i< num_workers;i++){
		total_matches+=worker_matches[i];
	}
	printf("Total matches found by all workers: %d\n",total_matches);

	return NULL;
}


int main(int argc, char *argv[]) {
    if (argc != 5) {
        fprintf(stderr, "Usage: %s <buffer_size> <num_workers> <log_file> <search_term>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    signal(SIGINT, sigint_handler);

    buffer_size = atoi(argv[1]);
    num_workers = atoi(argv[2]);
    fileName = argv[3];
    search_term = argv[4];

    worker_matches = malloc(num_workers * sizeof(int));
	for (int i = 0; i < num_workers; i++) {
	    worker_matches[i] = 0;  // Initialize each worker's match count to 0
	}

    lineBuffer = malloc(sizeof(char *) * buffer_size);
    if (lineBuffer == NULL) {
        perror("malloc");
        exit(EXIT_FAILURE);
    }
    pthread_t manager;
    pthread_t workers[num_workers];
    int worker_ids[num_workers];
    pthread_t final_report;

    pthread_barrier_init(&barrier, NULL, num_workers + 1); 

    pthread_create(&manager, NULL, th_manager, NULL);

    
    for (int i = 0; i < num_workers; i++) {
	   	int *thread_id = malloc(sizeof(int));
	    *thread_id = i + 1;
	    pthread_create(&workers[i], NULL, th_worker, thread_id);
	}
	sleep(1);
	pthread_create(&final_report,NULL,th_final_report,NULL);
	//printf("Final Report: Total matches found: %d\n", total_matches);
    pthread_join(manager, NULL);

    for (int i = 0; i < num_workers; i++) {
        pthread_join(workers[i], NULL);
    }

    pthread_join(final_report,NULL);

    free(lineBuffer);
    pthread_barrier_destroy(&barrier);
    free(worker_matches);
    return 0;
}
