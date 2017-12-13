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


#ifndef ITYPE_FACTORY_H
#define ITYPE_FACTORY_H


class IBlobContainer
{
public:
	virtual ~IBlobContainer() {}

	virtual unsigned char* Data() = 0;

	virtual unsigned long long Size() = 0;
};


class ITypeFactory
{
public:
	virtual ~ITypeFactory() {}

	/**
	 * \brief Create a BlobContainer type of size indicated.
	 * \param size Size of BlobContainer to create.
	 * \return Pointer to the generated BlobContainer object;
	 */
	virtual IBlobContainer* CreateBlobContainer(unsigned long long size) = 0;
};


#endif // ITYPE_FACTORY_H

