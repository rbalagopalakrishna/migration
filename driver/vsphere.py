import atexit
import os
import time
import ssl
 
from pyVim import connect
from pyVmomi import vim
from threading import Thread
from ssl import SSLError
 
def get_obj(content, vimtype):
    """Get VIMType Object.
 
       Return an object by name, if name is None the
       first found object is returned
    """
    container = content.viewManager.CreateContainerView(
        content.rootFolder, vimtype, True)
    return container.view
 
 
class VSphere:
 
    def __init__(self, creds):
        #self.context = context
        self.content = None
        self._initialize_connection()
 
    def _initialize_connection(self):
        try:
            con = connect.SmartConnect(host=creds['host'],
                                       user=creds['username'],
                                       pwd=creds['password'],
                                       port=int(443))
            atexit.register(connect.Disconnect, con)
            self.content = con.RetrieveContent()
        except SSLError:
            context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
            context.verify_mode = ssl.CERT_NONE
            con = connect.SmartConnect(host=creds['host'],
                                       user=creds['username'],
                                       pwd=creds['password'],
                                       port=int(443),
                                       sslContext=context)
            atexit.register(connect.Disconnect, con)
            self.content = con.RetrieveContent()
        except ConnectionRefusedError as error:
            print(("Creating a connection to the host: %(h)s has failed, "
                     "Error: %(err)s") % {'h': creds['host'], 'err': error})
        self._initialized = True 
 
    def _get_instance_disk(self, device_url, dest_disk_path):
        url = device_url.url
 
        # TODO: Remove this temp hack
        parsed_uri = urlparse(url)
        if parsed_uri.netloc == '*':
            host = str(self.cloud.params['host'])
            url = '{uri.scheme}://{host}{uri.path}'.format(uri=parsed_uri,host=host)
 
        if not os.path.exists(dest_disk_path):
            try:
                utils.execute('wget', url, '--no-check-certificate',
                              '-O', dest_disk_path, run_as_root=True)
            except Exception as error:
                msg = (_('Disk download failed. %s') % error.message)
                LOG.exception(msg)
                raise exception.VSphereException(msg)
 
    def _get_instance_lease(self, instance):
        lease = instance.ExportVm()
        count = 0
        while lease.state != 'ready':
            if count == 5:
                raise exception.VSphereException("Unable to take lease on sorce instance.")
            time.sleep(5)
            count += 1
        return lease
 
    def _get_device_urls(self, lease):
        try:
            device_urls = lease.info.deviceUrl
        except IndexError:
            time.sleep(2)
            device_urls = lease.info.deviceUrl
        return device_urls
 
    def _find_instance_by_uuid(self, instance_uuid):
        search_index = self.content.searchIndex
        instance = search_index.FindByUuid(None, instance_uuid,
                                           True, True)
 
        if instance is None:
            raise exception.VSphereException("Instance not found.")
        return instance

    def get_instance_list(self):
        if not self._initialized:
            self._initialize_connection()
        try:
            instances = get_obj(self.content, [vim.VirtualMachine])
            instance_list = []
            for instance in instances:
                if not instance.config:
                    continue
                i = {'name': instance.config.name,
                     'id_at_source': instance.config.instanceUuid}
                instance_list.append(i)
            return instance_list
        except AttributeError as error:
            print(error)
 
    def get_instance(self, instance_id, con_dir):
        if not self._initialized:
            self._initialize_connection()
        instance = self._find_instance_by_uuid(instance_id)
        # get flavor details.
        vm_disks = []
        for vm_hardware in instance.config.hardware.device:
            if (vm_hardware.key >= 2000) and (vm_hardware.key < 3000):
                vm_disks.append('{}'.format(vm_hardware.capacityInKB/1024/1024))
        disks = ','.join(vm_disks)
        root_disk = vm_disks[0]
        flavor_info = {'name': '%s_flavor' % instance.config.name,
                       'ram': instance.config.hardware.memoryMB,
                       'vcpus': instance.config.hardware.numCPU,
                       'disk': root_disk}
        guest_os_name = instance.config.guestFullName
        powerState = instance.runtime.powerState
        poweredOn = vim.VirtualMachinePowerState.poweredOn
 
        if powerState == poweredOn:
            task = instance.PowerOff()
            while task.info.state not in [vim.TaskInfo.State.success,
                                          vim.TaskInfo.State.error]:
                time.sleep(1)
        lease = self._get_instance_lease(instance)
 
        def keep_lease_alive(lease):
            """Keeps the lease alive while GETing the VMDK."""
            while(True):
                time.sleep(5)
                try:
                    # Choosing arbitrary percentage to keep the lease alive.
                    lease.HttpNfcLeaseProgress(50)
                    if (lease.state == vim.HttpNfcLease.State.done):
                        return
                    # If the lease is released, we get an exception.
                    # Returning to kill the thread.
                except Exception as error:
                    raise exception.VSphereException(error.message)
        disks = []
        image_info = {}
        volume_info = []
        try:
            if lease.state == vim.HttpNfcLease.State.ready:
                keepalive_thread = Thread(target=keep_lease_alive,
                                          args=(lease,))
 
                keepalive_thread.daemon = True
                keepalive_thread.start()
                device_urls = self._get_device_urls(lease)
 
                for device_url in device_urls:
                    data = {}
                    path = os.path.join(con_dir,
                                        device_url.targetId)
                    self._get_instance_disk(device_url, path)
                    index = device_url.key.split(':')[1]
                    path = utils.convert_vmdk_to_qcow2(path)
                    if index == '0':
                        image_info = {"name": "%s_image_%s" % (instance.name, index),
                                      "local_image_path": path}
                    else:
                        vol_image_info = {"name": "%s_vol_%s" % (instance.name, index),
                                          "local_image_path": path}
                        disk = utils.qemu_img_info(path)
                        vol = {'image_info': vol_image_info,
                               'size': disk.virtual_size/(1024*1024*1024),
                               'name': "%s_vol_%s" % (instance.name, index),
                               'display_name': "%s_vol_%s" % (instance.name, index)}
                        volume_info.append(vol)
 
                lease.HttpNfcLeaseComplete()
                keepalive_thread.join()
            elif lease.state == vim.HttpNfcLease.State.error:
                raise exception.VSphereException("Failed to download disk.")
            else:
                raise exception.VSphereException("Failed to download disk.")
        except Exception:
            raise exception.VSphereException('Failed to download disk.')
 
        return {'name': instance.name,
                'image_info': image_info,
                'flavor_info': flavor_info,
                'volume_info': volume_info}

creds = {'host': '127.0.0.1', 'username': 'username', 'password': 'password'}
obj = VSphere(creds)
obj.get_instance_list()
