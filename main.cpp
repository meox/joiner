#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include <map>
#include <thread>

#include <fcntl.h>
#include <unistd.h>

// ./joiner --generate 10 --prefix /tmp/foo --size 104857600
// ./joiner --join 10 --prefix /tmp/foo --out /tmp/foo_all.dat

void generate(const std::string& prefix, unsigned long n, unsigned long size);
void joiner(const std::string& prefix, unsigned long n, bool parallel, bool preallocate, size_t max_thread, const std::string& out_file_name);
std::map<unsigned long, std::vector<unsigned long>> groups(unsigned long n, size_t max_thread);

enum struct action_t{NONE, GENERATE, JOIN};

int main(int argc, char *argv[])
{
  int i = 0;
  unsigned long n = 1, size = 1024;
  size_t max_threads{64};
  action_t action = action_t::NONE;
  std::string prefix{"./"}, out_file_name;
  bool parallel{false}, preallocate{false};

  while (i < argc)
  {
    if (strcmp(argv[i], "--generate") == 0)
    {
      action = action_t::GENERATE;
      n = std::stoul(argv[i + 1]);
      i += 2;
      continue;
    }

    if (strcmp(argv[i], "--join") == 0)
    {
      action = action_t::JOIN;
      n = std::stoul(argv[i + 1]);
      i += 2;
      continue;
    }

    if (strcmp(argv[i], "--prefix") == 0)
    {
      prefix = argv[i + 1];
      i += 2;
      continue;
    }

    if (strcmp(argv[i], "--out") == 0)
    {
      out_file_name = argv[i + 1];
      i += 2;
      continue;
    }

    if (strcmp(argv[i], "--size") == 0)
    {
      size = std::stoul(argv[i + 1]);
      i += 2;
      continue;
    }

    if (strcmp(argv[i], "--parallel") == 0)
    {
      parallel = true;
      i += 1;
      continue;
    }

    if (strcmp(argv[i], "--preallocate") == 0)
    {
      preallocate = true;
      i += 1;
      continue;
    }

    if (strcmp(argv[i], "--max-threads") == 0)
    {
      max_threads = std::stoul(argv[i + 1]);
      i += 2;
      continue;
    }

    i++;
  }

  switch (action)
  {
    case action_t::NONE:
    {
      std::cerr << "No action specified\n";
      return 1;
    }
    case action_t::GENERATE:
    {
      generate(prefix, n, size);
      break;
    }
    case action_t::JOIN:
    {
      joiner(prefix, n, parallel, preallocate, max_threads, out_file_name);
      break;
    }
  }

  return 0;
}


void generate(const std::string& prefix, unsigned long n, unsigned long size)
{
  char* data = new char[size];

  // read random data from /dev/urandom
  std::ifstream r{"/dev/urandom", std::ios::in | std::ios::binary};
  r.read(&data[0], size);
  r.close();

  for (unsigned long i = 0; i < n; i++)
  {
    const std::string final = prefix + "_" + std::to_string(i);
    std::cout << "writing into: " << final << std::endl;
    std::ofstream out{final, std::ios::out | std::ios::binary};
    if (!out.is_open())
    {
      std::cerr << "cannot read: " << final << "\n";
      return;
    }

    out.write(&data[0], size);
    out.close();
  }

  delete []data;
}


void joiner(const std::string& prefix, unsigned long n, bool parallel, bool preallocate, size_t max_threads, const std::string& out_file_name)
{
  unsigned long total_size{};

  size_t part_size{};
  std::vector<std::ifstream> ins;
  for (unsigned long i = 0; i < n; i++)
  {
    const std::string infname = prefix + "_" + std::to_string(i);
    ins.push_back(std::ifstream(infname, std::ios::binary | std::ios::ate));
    auto &f = ins[ins.size() - 1];
    part_size = f.tellg();
    total_size += part_size;
    f.seekg(0);
  }

  if (parallel)
  {
    std::cout << "PARALLEL: file=" << out_file_name << ", file_size=" << total_size << std::endl;

    // preallocate the output file
    auto fd = open(out_file_name.c_str(), O_WRONLY | O_CLOEXEC);
    fallocate(fd, 0, 0, total_size);
    close(fd);

    std::vector<std::thread> wg;
    for(const auto& [k, v] : groups(n, max_threads))
    {
      wg.emplace_back(std::thread([&ins, part_size, &out_file_name](const auto& vs) {
        const size_t max_buff = 64*1024;
        char * buff = new char[max_buff];

        for (const auto idx : vs)
        {
          auto &f = ins[idx];
          const auto offset = idx * part_size;

          //seek
          auto fd = open(out_file_name.c_str(), O_WRONLY | O_CLOEXEC);
          lseek(fd, offset, 0);

          //copy
          while(true)
          {
            const auto r = f.readsome(&buff[0], max_buff);
            if (r <= 0)
              break;

            const auto w = write(fd, &buff[0], r);
            if (w != r)
            {
              std::cerr << "error writing out file w=" << w << ", r=" << r << std::endl;
              exit(1);
            }
          }

          // close (origin and dest)
          f.close();
          close(fd);
        }

        delete []buff;
      }, v));
    }

    std::cout << "spawned " << wg.size() << " threads\n";
    for (auto &th : wg)
      th.join();
  }
  else
  {
    std::cout << "SERIAL: file_size=" << out_file_name << ", total_size=" << total_size << std::endl;

    auto * fout = fopen(out_file_name.c_str(), "wb");
    auto fd = fileno(fout);

    if (preallocate)
      fallocate(fd, 0, 0, total_size);

    const size_t max_buff = 64*1024;
    char * buff = new char[max_buff];

    for(auto& f : ins)
    {
      while(true)
      {
        const auto r = f.readsome(&buff[0], max_buff);
        if (r <= 0)
          break;

        fwrite(&buff[0], 1, r, fout);
      }

      f.close();
    }

    fclose(fout);
    fsync(fd);
  }
}

std::map<unsigned long, std::vector<unsigned long>> groups(unsigned long n, size_t max_thread)
{
  std::map<unsigned long, std::vector<unsigned long>> map_workers;

  for (unsigned long i = 0; i < n; i++)
  {
    const auto idx = i % max_thread;
    map_workers[idx].push_back(i);
  }

  return map_workers;
}
