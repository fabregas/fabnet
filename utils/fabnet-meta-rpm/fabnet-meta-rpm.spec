%define name fabnet-meta
%define version vNNN
%define release rNNN
%define _topdir /tmp/build/rpm
%define _tmppath /tmp

Summary: Fabnet meta package
Name: %{name}
Version: %{version}
Release: %{release}
License: GPLv3
Group: Development/Application
BuildRoot: %{_tmppath}/%{name}-%{version}-buildroot
BuildArch: noarch
Vendor: Blik

Requires: python >= 2.6
Requires: git

%description
 - Yum repository configuration

%prep

%build
mkdir -p $RPM_BUILD_ROOT/etc/yum.repos.d/

cp fabnet.repo $RPM_BUILD_ROOT/etc/yum.repos.d/

%preun

%postun

%clean
if [ $RPM_BUILD_ROOT != '/' ]; then rm -rf $RPM_BUILD_ROOT; fi

%files
%attr(555,root,root) /etc/yum.repos.d/fabnet.repo
