%define	   repo_file paradigm4.repo
Name:      paradigm4-repo
Summary:   SciDB download repository configuration
License:   GPLv3
Version:   VERSION
Release:   0
Group:     System Environment/Base
URL:       https://downloads.paradigm4.com
Source0:   %{repo_file}
BuildRoot: %{_tmppath}/%{name}-root
BuildArch: noarch

%description
SciDB download repository configuration

%prep
cp %{_sourcedir}/%{repo_file} %{_builddir}/%{repo_file}

%build

%install
rm -rf %{buildroot}
install -dm 755 %{buildroot}%{_sysconfdir}/yum.repos.d
install -pm 644 %{_builddir}/%{repo_file} %{buildroot}/%{_sysconfdir}/yum.repos.d

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%config /etc/yum.repos.d/*

%changelog
* Thu Mar  5 2015 SciDB support list <support@lists.scidb.org>
- Initial build.
