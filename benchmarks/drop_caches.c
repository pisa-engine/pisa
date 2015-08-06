#include <unistd.h>
#include <fcntl.h>

int main()
{
    sync();

    int fd = open("/proc/sys/vm/drop_caches", O_WRONLY);
    if (!fd) {
        return 1;
    }
    if (write(fd, "1\n", 2) != 2) {
        return 2;
    }

    close(fd);

    sync();
    
    return 0;
}
