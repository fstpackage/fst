/*
  fstlib - A C++ library for ultra fast storage and retrieval of datasets

  Copyright (C) 2017-present, Mark AJ Klik

  This file is part of fstlib.

  fstlib is free software: you can redistribute it and/or modify it under the
  terms of the GNU Affero General Public License version 3 as published by the
  Free Software Foundation.

  fstlib is distributed in the hope that it will be useful, but WITHOUT ANY
  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
  A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
  details.

  You should have received a copy of the GNU Affero General Public License
  along with fstlib. If not, see <http://www.gnu.org/licenses/>.

  You can contact the author at:
  - fstlib source repository : https://github.com/fstpackage/fstlib
*/


#ifndef FST_DB_H
#define FST_DB_H

// std includes
#include <vector>

// fst includes
#include "ifsttable.h"


class FstDb;

class FstTableProxy
{
public:

  FstTableProxy(FstDb fst_db, std::string table_name);

  ~FstTableProxy();

  void rename(std::string table_name);

  void arrange();
  void distinct();
  void do();
  void filter();
  void intersect();
  void mutate();
  void select();
  void pull();
  void sample_frac();
  void sample_n();
  void setdiff();
  void setequal();
  void slice();
  void summarise();
  void union();
  void union_all();

  void group_by();
  void group_indices();
  void group_size();
  void groups();
  void n_groups();
  void ungroup();

  void anti_join();
  void full_join();
  void inner_join();
  void left_join();
  void right_join();
  void semi_join();

  void collapse();
  void compute();
};


// A database of fst files with hierachiel structure
class FstDb
{

  public:

    FstDb(std::string path);

    ~FstDb() { }

    void add_table(IFstTable table, std::string table_name) const;

    void remove_table(std::string table_name) const;

    std::vector<table_info> table_index(std::string path) const;
};


#endif  // FST_DB_H
