Name:      scidb-VERSION-installer
Summary:   SciDB download installer configuration
Version:   0
Release:   1
License:   GPLv3
Group:     System Environment/Base
URL:       https://downloads.paradigm4.com
Source0:   config.ini
Source1:   fixes.sh
Source2:   create_scidb_user.sh
Source3:   setup_postgresql.sh
Source4:   run_scidb.sh
Source5:   README.installer
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-root
BuildArch: noarch
Requires: scidb-VERSION-all-coord

%description
SciDB installer

%prep
cp %{_sourcedir}/config.ini %{_builddir}/config.ini
cp %{_sourcedir}/fixes.sh %{_builddir}/fixes.sh
cp %{_sourcedir}/create_scidb_user.sh %{_builddir}/create_scidb_user.sh
cp %{_sourcedir}/setup_postgresql.sh %{_builddir}/setup_postgresql.sh
cp %{_sourcedir}/run_scidb.sh %{_builddir}/run_scidb.sh
cp %{_sourcedir}/README.installer %{_builddir}/README.installer

%build

%install
rm -rf %{buildroot}
install -dm 755 %{buildroot}/opt/scidb/VERSION/script
install -pm 644 %{_builddir}/config.ini %{buildroot}/opt/scidb/VERSION/script
install -pm 644 %{_builddir}/fixes.sh %{buildroot}/opt/scidb/VERSION/script
install -pm 644 %{_builddir}/create_scidb_user.sh %{buildroot}/opt/scidb/VERSION/script
install -pm 644 %{_builddir}/setup_postgresql.sh %{buildroot}/opt/scidb/VERSION/script
install -pm 644 %{_builddir}/run_scidb.sh %{buildroot}/opt/scidb/VERSION/script
install -pm 644 %{_builddir}/README.installer %{buildroot}/opt/scidb/VERSION/script

%clean

%files
%defattr(-,root,root,-)
%attr(755,root,root) /opt/scidb/VERSION/script/*

%post
/opt/scidb/VERSION/script/fixes.sh
/opt/scidb/VERSION/script/create_scidb_user.sh
/opt/scidb/VERSION/script/setup_postgresql.sh
/opt/scidb/VERSION/script/run_scidb.sh

%changelog
* Thu Mar  5 2015 SciDB support list <support@lists.scidb.org>
- Initial build.
